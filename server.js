import express from 'express';
import fetch from 'node-fetch';

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const WORKER_SECRET = process.env.WORKER_SECRET_KEY || 'your-secret-key';

// Health check endpoint
app.get('/', (req, res) => {
  res.json({ status: 'Shopify Sync Worker is running' });
});

// Webhook endpoint that receives sync requests
app.post('/sync-shopify', async (req, res) => {
  // Verify secret
  const authHeader = req.headers.authorization;
  if (authHeader !== `Bearer ${WORKER_SECRET}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const { integrationId, accountId, userEmail, config, base44AppId, base44ApiUrl } = req.body;

  console.log(`📥 Received sync request for integration: ${integrationId}`);

  // Respond immediately - sync will run in background
  res.status(202).json({ 
    message: 'Sync started',
    integrationId 
  });

  // Run sync in background
  runShopifySync(integrationId, accountId, userEmail, config, base44AppId, base44ApiUrl);
});

// Main sync function
async function runShopifySync(integrationId, accountId, userEmail, config, base44AppId, base44ApiUrl) {
  console.log(`🚀 Starting background sync for integration: ${integrationId}`);
  
  const startTime = Date.now();

  try {
    const accessToken = config.accessToken || config.apiKey;
    let storeUrl = config.storeUrl;

    if (!accessToken || !storeUrl) {
      throw new Error('Shopify configuration is incomplete.');
    }

    storeUrl = storeUrl.replace(/https?:\/\//, '').replace(/\/$/, '');

    // Update progress: Testing connection
    await updateIntegration(integrationId, base44AppId, base44ApiUrl, {
      sync_progress: {
        current_step: 'Testing connection',
        last_updated: new Date().toISOString()
      }
    });

    console.log('Testing Shopify connection...');
    const shopResponse = await fetch(`https://${storeUrl}/admin/api/2024-04/shop.json`, {
      headers: { 'X-Shopify-Access-Token': accessToken }
    });

    if (!shopResponse.ok) {
      const errorText = await shopResponse.text();
      throw new Error(`Shopify connection failed (${shopResponse.status}): ${errorText}`);
    }

    const shopData = await shopResponse.json();
    const shopName = shopData.shop.name;
    console.log(`✅ Connected to: ${shopName}`);

    // Sync customers
    console.log('📊 Syncing customers...');
    const customersSynced = await syncCustomers(storeUrl, accessToken, accountId, integrationId, base44AppId, base44ApiUrl);

    // Sync orders
    console.log('🛍️ Syncing orders...');
    const ordersSynced = await syncOrders(storeUrl, accessToken, accountId, integrationId, base44AppId, base44ApiUrl);

    // Sync products
    console.log('📦 Syncing products...');
    const productsSynced = await syncProducts(storeUrl, accessToken, accountId, integrationId, base44AppId, base44ApiUrl);

    // Update customer totals
    console.log('💰 Calculating customer totals...');
    await updateIntegration(integrationId, base44AppId, base44ApiUrl, {
      sync_progress: {
        current_step: 'Finalizing sync',
        last_updated: new Date().toISOString()
      }
    });
    
    await updateCustomerTotals(accountId, integrationId, base44AppId, base44ApiUrl);

    const durationMinutes = Math.round((Date.now() - startTime) / 60000);

    // Mark as complete
    await updateIntegration(integrationId, base44AppId, base44ApiUrl, {
      status: 'connected',
      last_sync: new Date().toISOString(),
      total_records: customersSynced + ordersSynced + productsSynced,
      sync_progress: null
    });

    // Send success email
    await sendEmail(base44AppId, base44ApiUrl, {
      to: userEmail,
      subject: `✅ Shopify Sync Complete - ${shopName}`,
      body: `
        <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
          <h2 style="color: #00D563;">✅ Shopify Sync Complete!</h2>
          <p>Your <strong>${shopName}</strong> data has been successfully synced.</p>
          
          <div style="background: #f6f8fa; padding: 20px; border-radius: 8px; margin: 20px 0;">
            <h3 style="margin-top: 0;">Sync Summary</h3>
            <ul style="list-style: none; padding: 0;">
              <li>📊 <strong>${customersSynced.toLocaleString()}</strong> customers</li>
              <li>🛍️ <strong>${ordersSynced.toLocaleString()}</strong> orders</li>
              <li>📦 <strong>${productsSynced.toLocaleString()}</strong> products</li>
            </ul>
            <p style="color: #656D76; font-size: 14px; margin-top: 15px;">
              ⏱️ Sync completed in ${durationMinutes} minutes
            </p>
          </div>
          
          <a href__="https://preview--customer-ai-nexus-830104b7.base44.app/Dashboard" 
             style="display: inline-block; background: #00D563; color: white; padding: 12px 24px; 
                    text-decoration: none; border-radius: 8px; font-weight: 600; margin-top: 10px;">
            View Dashboard
          </a>
        </div>
      `
    });

    console.log(`✅ Sync completed successfully in ${durationMinutes} minutes`);

  } catch (error) {
    console.error('❌ Sync error:', error);

    // Update integration status to error
    await updateIntegration(integrationId, base44AppId, base44ApiUrl, {
      status: 'error',
      sync_progress: null
    }).catch(e => console.error('Failed to update integration status:', e));

    // Send failure email
    await sendEmail(base44AppId, base44ApiUrl, {
      to: userEmail,
      subject: '❌ Shopify Sync Failed',
      body: `
        <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
          <h2 style="color: #DC2626;">❌ Shopify Sync Failed</h2>
          <p>Unfortunately, your Shopify sync encountered an error.</p>
          
          <div style="background: #FEF2F2; padding: 20px; border-radius: 8px; margin: 20px 0; border-left: 4px solid #DC2626;">
            <h3 style="margin-top: 0; color: #DC2626;">Error Details</h3>
            <p style="font-family: monospace; font-size: 14px; color: #7F1D1D;">${error.message}</p>
          </div>
          
          <p>Please try the following:</p>
          <ul>
            <li>Wait a few minutes and try the sync again</li>
            <li>Verify your Shopify credentials are correct</li>
            <li>Check that your API key has the required permissions</li>
          </ul>
          
          <a href__="https://preview--customer-ai-nexus-830104b7.base44.app/Integrations" 
             style="display: inline-block; background: #DC2626; color: white; padding: 12px 24px; 
                    text-decoration: none; border-radius: 8px; font-weight: 600; margin-top: 10px;">
            Go to Integrations
          </a>
        </div>
      `
    }).catch(e => console.error('Failed to send error email:', e));
  }
}

// Helper: Update integration in Base44
async function updateIntegration(integrationId, appId, apiUrl, data) {
  const response = await fetch(`${apiUrl}/v1/apps/${appId}/entities/Integration/${integrationId}`, {
    method: 'PATCH',
    headers: {
      'Content-Type': 'application/json',
      'x-app-id': appId
    },
    body: JSON.stringify(data)
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Failed to update integration: ${response.status} - ${errorText}`);
  }

  return response.json();
}

// Helper: Shopify GraphQL query with retry
async function shopifyGraphQLQuery(query, variables, storeUrl, accessToken, retries = 3) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const response = await fetch(`https://${storeUrl}/admin/api/2024-04/graphql.json`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Shopify-Access-Token': accessToken,
        },
        body: JSON.stringify({ query, variables }),
      });

      if (!response.ok) {
        throw new Error(`GraphQL API Error: ${response.status}`);
      }

      const result = await response.json();

      if (result.errors) {
        const throttleError = result.errors.find(e => e.extensions?.code === 'THROTTLED');
        if (throttleError && attempt < retries) {
          const waitTime = Math.pow(2, attempt + 1) * 1000;
          console.log(`⏳ Rate limited. Waiting ${waitTime}ms before retry ${attempt + 1}/${retries}...`);
          await new Promise(resolve => setTimeout(resolve, waitTime));
          continue;
        }
        throw new Error(`GraphQL errors: ${JSON.stringify(result.errors)}`);
      }

      return result;
    } catch (error) {
      if (attempt === retries) throw error;
      const waitTime = Math.pow(2, attempt + 1) * 1000;
      console.log(`Query failed, retrying in ${waitTime}ms...`);
      await new Promise(resolve => setTimeout(resolve, waitTime));
    }
  }
}

// Sync customers
async function syncCustomers(storeUrl, accessToken, accountId, integrationId, appId, apiUrl) {
  let hasNextPage = true;
  let cursor = null;
  let totalSynced = 0;
  let totalCount = 0;

  const query = `
    query($cursor: String) {
      customers(first: 50, after: $cursor) {
        edges {
          node {
            id
            firstName
            lastName
            email
            phone
            updatedAt
            defaultAddress {
              city
              country
              province
            }
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
  `;

  // First pass: collect all customer data
  let allCustomers = [];
  
  do {
    const { data } = await shopifyGraphQLQuery(query, { cursor }, storeUrl, accessToken);
    hasNextPage = data.customers.pageInfo.hasNextPage;
    cursor = data.customers.pageInfo.endCursor;

    allCustomers.push(...data.customers.edges);

    if (hasNextPage) {
      await new Promise(resolve => setTimeout(resolve, 500));
    }
  } while (hasNextPage);

  totalCount = allCustomers.length;
  console.log(`Found ${totalCount} customers to sync`);

  // Update with total count
  await updateIntegration(integrationId, appId, apiUrl, {
    sync_progress: {
      current_step: 'Syncing customers',
      customers_total: totalCount,
      customers_processed: 0,
      last_updated: new Date().toISOString()
    }
  });

  // Second pass: create/update in batches
  for (let i = 0; i < allCustomers.length; i++) {
    const edge = allCustomers[i];
    const customer = edge.node;
    
    if (!customer || !customer.id) continue;

    const shopifyId = customer.id.split('/').pop();
    let location = null;
    
    if (customer.defaultAddress) {
      location = {
        city: customer.defaultAddress.city,
        country: customer.defaultAddress.country,
        timezone: null,
        state: customer.defaultAddress.province
      };
    }

    const customerData = {
      account_id: accountId,
      integration_id: integrationId,
      customer_id: shopifyId,
      platform: 'Shopify',
      full_name: `${customer.firstName || ''} ${customer.lastName || ''}`.trim(),
      email: customer.email,
      phone: customer.phone,
      total_value: 0,
      last_activity: customer.updatedAt,
      status: 'new',
      engagement_score: 10,
      location
    };

    try {
      await fetch(`${apiUrl}/v1/apps/${appId}/entities/Customer`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-app-id': appId
        },
        body: JSON.stringify(customerData)
      });

      totalSynced++;

      if (totalSynced % 100 === 0 || i === allCustomers.length - 1) {
        await updateIntegration(integrationId, appId, apiUrl, {
          sync_progress: {
            current_step: 'Syncing customers',
            customers_total: totalCount,
            customers_processed: totalSynced,
            last_updated: new Date().toISOString()
          }
        });
      }
    } catch (error) {
      console.error(`Failed to create customer ${shopifyId}:`, error.message);
    }

    if (i % 10 === 0) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }

  return totalSynced;
}

// Sync orders
async function syncOrders(storeUrl, accessToken, accountId, integrationId, appId, apiUrl) {
  let hasNextPage = true;
  let cursor = null;
  let totalSynced = 0;

  const query = `
    query($cursor: String) {
      orders(first: 25, sortKey: CREATED_AT, reverse: true, after: $cursor) {
        edges {
          node {
            id
            name
            createdAt
            totalPriceSet { shopMoney { amount } }
            customer { id email }
            lineItems(first: 20) {
              edges {
                node {
                  title
                  quantity
                  originalTotalSet { shopMoney { amount } }
                }
              }
            }
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
  `;

  let allOrders = [];
  
  do {
    const { data } = await shopifyGraphQLQuery(query, { cursor }, storeUrl, accessToken);
    hasNextPage = data.orders.pageInfo.hasNextPage;
    cursor = data.orders.pageInfo.endCursor;

    allOrders.push(...data.orders.edges);

    if (hasNextPage) {
      await new Promise(resolve => setTimeout(resolve, 500));
    }
  } while (hasNextPage);

  const totalCount = allOrders.length;
  console.log(`Found ${totalCount} orders to sync`);

  await updateIntegration(integrationId, appId, apiUrl, {
    sync_progress: {
      current_step: 'Syncing orders',
      orders_total: totalCount,
      orders_processed: 0,
      last_updated: new Date().toISOString()
    }
  });

  for (let i = 0; i < allOrders.length; i++) {
    const edge = allOrders[i];
    const order = edge.node;
    
    if (!order || !order.id || !order.customer) continue;

    const shopifyOrderId = order.id.split('/').pop();
    const shopifyCustomerId = order.customer.id.split('/').pop();

    const lineItems = order.lineItems.edges.map(itemEdge => ({
      title: itemEdge.node.title,
      quantity: itemEdge.node.quantity,
      price: parseFloat(itemEdge.node.originalTotalSet.shopMoney.amount)
    }));

    const interactionData = {
      account_id: accountId,
      integration_id: integrationId,
      customer_id: shopifyCustomerId,
      interaction_type: 'purchase',
      platform: 'Shopify',
      title: `Shopify Order ${order.name}`,
      description: `Online store purchase with ${lineItems.length} item(s): ${lineItems.map(li => `${li.quantity}x ${li.title}`).join(', ')}.`,
      value: parseFloat(order.totalPriceSet?.shopMoney?.amount || 0),
      outcome: 'positive',
      interaction_date: order.createdAt,
      metadata: { order_id: shopifyOrderId, order_name: order.name },
      line_items: lineItems
    };

    try {
      await fetch(`${apiUrl}/v1/apps/${appId}/entities/Interaction`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-app-id': appId
        },
        body: JSON.stringify(interactionData)
      });

      totalSynced++;

      if (totalSynced % 100 === 0 || i === allOrders.length - 1) {
        await updateIntegration(integrationId, appId, apiUrl, {
          sync_progress: {
            current_step: 'Syncing orders',
            orders_total: totalCount,
            orders_processed: totalSynced,
            last_updated: new Date().toISOString()
          }
        });
      }
    } catch (error) {
      console.error(`Failed to create order ${shopifyOrderId}:`, error.message);
    }

    if (i % 10 === 0) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }

  return totalSynced;
}

// Sync products
async function syncProducts(storeUrl, accessToken, accountId, integrationId, appId, apiUrl) {
  let hasNextPage = true;
  let cursor = null;
  let totalSynced = 0;

  const query = `
    query($cursor: String) {
      products(first: 50, after: $cursor) {
        edges {
          node {
            id
            title
            description
            vendor
            productType
            status
            tags
            images(first: 5) {
              nodes {
                url
              }
            }
            variants(first: 1) {
              nodes {
                price
                compareAtPrice
                inventoryQuantity
              }
            }
            variantsCount {
              count
            }
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
  `;

  let allProducts = [];
  
  do {
    const { data } = await shopifyGraphQLQuery(query, { cursor }, storeUrl, accessToken);
    hasNextPage = data.products.pageInfo.hasNextPage;
    cursor = data.products.pageInfo.endCursor;

    allProducts.push(...data.products.edges);

    if (hasNextPage) {
      await new Promise(resolve => setTimeout(resolve, 500));
    }
  } while (hasNextPage);

  const totalCount = allProducts.length;
  console.log(`Found ${totalCount} products to sync`);

  await updateIntegration(integrationId, appId, apiUrl, {
    sync_progress: {
      current_step: 'Syncing products',
      products_total: totalCount,
      products_processed: 0,
      last_updated: new Date().toISOString()
    }
  });

  for (let i = 0; i < allProducts.length; i++) {
    const edge = allProducts[i];
    const product = edge.node;
    
    if (!product || !product.id) continue;

    const shopifyId = product.id.split('/').pop();
    const firstVariant = product.variants.nodes[0];

    const productData = {
      account_id: accountId,
      integration_id: integrationId,
      product_id: shopifyId,
      title: product.title,
      description: product.description || '',
      vendor: product.vendor,
      product_type: product.productType,
      price: firstVariant ? parseFloat(firstVariant.price) : 0,
      compare_at_price: firstVariant ? parseFloat(firstVariant.compareAtPrice || 0) : 0,
      inventory_quantity: firstVariant ? (firstVariant.inventoryQuantity || 0) : 0,
      platform: 'Shopify',
      status: product.status.toLowerCase(),
      tags: product.tags || [],
      images: product.images.nodes.map(img => img.url),
      variants_count: product.variantsCount.count
    };

    try {
      await fetch(`${apiUrl}/v1/apps/${appId}/entities/Product`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-app-id': appId
        },
        body: JSON.stringify(productData)
      });

      totalSynced++;

      if (totalSynced % 100 === 0 || i === allProducts.length - 1) {
        await updateIntegration(integrationId, appId, apiUrl, {
          sync_progress: {
            current_step: 'Syncing products',
            products_total: totalCount,
            products_processed: totalSynced,
            last_updated: new Date().toISOString()
          }
        });
      }
    } catch (error) {
      console.error(`Failed to create product ${shopifyId}:`, error.message);
    }

    if (i % 10 === 0) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }

  return totalSynced;
}

// Update customer totals
async function updateCustomerTotals(accountId, integrationId, appId, apiUrl) {
  console.log('Calculating customer totals...');
  
  try {
    const [customersRes, interactionsRes] = await Promise.all([
      fetch(`${apiUrl}/v1/apps/${appId}/entities/Customer?account_id=${accountId}&integration_id=${integrationId}&limit=10000`, {
        headers: { 'x-app-id': appId }
      }),
      fetch(`${apiUrl}/v1/apps/${appId}/entities/Interaction?account_id=${accountId}&integration_id=${integrationId}&interaction_type=purchase&limit=10000`, {
        headers: { 'x-app-id': appId }
      })
    ]);

    const customers = await customersRes.json();
    const interactions = await interactionsRes.json();

    console.log(`Loaded ${customers.length} customers and ${interactions.length} interactions`);

    const customerTotals = {};
    interactions.forEach(interaction => {
      if (interaction.customer_id && interaction.value) {
        customerTotals[interaction.customer_id] = (customerTotals[interaction.customer_id] || 0) + interaction.value;
      }
    });

    let updated = 0;
    for (const customer of customers) {
      const total = customerTotals[customer.customer_id] || 0;
      
      if (total > 0) {
        try {
          await fetch(`${apiUrl}/v1/apps/${appId}/entities/Customer/${customer.id}`, {
            method: 'PATCH',
            headers: {
              'Content-Type': 'application/json',
              'x-app-id': appId
            },
            body: JSON.stringify({ total_value: total })
          });
          updated++;
        } catch (error) {
          console.error(`Failed to update customer ${customer.id}:`, error.message);
        }
      }

      if (updated % 10 === 0) {
        await new Promise(resolve => setTimeout(resolve, 100));
      }
    }

    console.log(`Updated ${updated} customer totals`);
    return updated;
  } catch (error) {
    console.error('Error updating customer totals:', error);
    throw error;
  }
}

// Send email via Base44
async function sendEmail(appId, apiUrl, { to, subject, body }) {
  try {
    await fetch(`${apiUrl}/v1/apps/${appId}/integrations/Core/SendEmail`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-app-id': appId
      },
      body: JSON.stringify({ to, subject, body })
    });
  } catch (error) {
    console.error('Failed to send email:', error);
  }
}

app.listen(PORT, () => {
  console.log(`🚀 Shopify Sync Worker running on port ${PORT}`);
});
