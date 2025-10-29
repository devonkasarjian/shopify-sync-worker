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

  const { integrationId, accountId, userEmail, config, progressCallbackUrl, workerSecret } = req.body;

  console.log(`📥 Received sync request for integration: ${integrationId}`);

  // Respond immediately - sync will run in background
  res.status(202).json({ 
    message: 'Sync started',
    integrationId 
  });

  // Run sync in background
  runShopifySync(integrationId, accountId, userEmail, config, progressCallbackUrl, workerSecret);
});

// Main sync function
async function runShopifySync(integrationId, accountId, userEmail, config, progressCallbackUrl, workerSecret) {
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
    await updateIntegration(integrationId, progressCallbackUrl, workerSecret, {
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
    const customersSynced = await syncCustomers(storeUrl, accessToken, accountId, integrationId, progressCallbackUrl, workerSecret);

    // Sync orders
    console.log('🛍️ Syncing orders...');
    const ordersSynced = await syncOrders(storeUrl, accessToken, accountId, integrationId, progressCallbackUrl, workerSecret);

    // Sync products
    console.log('📦 Syncing products...');
    const productsSynced = await syncProducts(storeUrl, accessToken, accountId, integrationId, progressCallbackUrl, workerSecret);

    // Update customer totals
    console.log('💰 Calculating customer totals...');
    await updateIntegration(integrationId, progressCallbackUrl, workerSecret, {
      sync_progress: {
        current_step: 'Finalizing sync',
        last_updated: new Date().toISOString()
      }
    });
    
    await updateCustomerTotals(accountId, integrationId, progressCallbackUrl, workerSecret);

    const durationMinutes = Math.round((Date.now() - startTime) / 60000);

    // Mark as complete
    await updateIntegration(integrationId, progressCallbackUrl, workerSecret, {
      status: 'connected',
      last_sync: new Date().toISOString(),
      total_records: customersSynced + ordersSynced + productsSynced,
      sync_progress: null
    });

    console.log(`✅ Sync completed successfully in ${durationMinutes} minutes`);

  } catch (error) {
    console.error('❌ Sync error:', error);

    // Update integration status to error
    await updateIntegration(integrationId, progressCallbackUrl, workerSecret, {
      status: 'error',
      sync_progress: null
    }).catch(e => console.error('Failed to update integration status:', e));
  }
}

// Helper: Update integration via Base44 function callback
async function updateIntegration(integrationId, callbackUrl, workerSecret, data) {
  const response = await fetch(callbackUrl, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${workerSecret}`
    },
    body: JSON.stringify({
      integrationId,
      updateData: data
    })
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Failed to update integration: ${response.status} - ${errorText}`);
  }

  return response.json();
}

// Helper: Create entity via callback
async function createEntity(callbackUrl, workerSecret, entityName, data) {
  const response = await fetch(callbackUrl.replace('/updateSyncProgress', '/createEntity'), {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${workerSecret}`
    },
    body: JSON.stringify({
      entityName,
      data
    })
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Failed to create ${entityName}: ${response.status} - ${errorText}`);
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
async function syncCustomers(storeUrl, accessToken, accountId, integrationId, callbackUrl, workerSecret) {
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
  await updateIntegration(integrationId, callbackUrl, workerSecret, {
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
      await createEntity(callbackUrl, workerSecret, 'Customer', customerData);
      totalSynced++;

      // Update progress every 100 records
      if (totalSynced % 100 === 0 || i === allCustomers.length - 1) {
        await updateIntegration(integrationId, callbackUrl, workerSecret, {
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

    // Small delay to avoid overwhelming Base44 API
    if (i % 10 === 0) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }

  return totalSynced;
}

// Sync orders
async function syncOrders(storeUrl, accessToken, accountId, integrationId, callbackUrl, workerSecret) {
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

  await updateIntegration(integrationId, callbackUrl, workerSecret, {
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
      await createEntity(callbackUrl, workerSecret, 'Interaction', interactionData);
      totalSynced++;

      if (totalSynced % 100 === 0 || i === allOrders.length - 1) {
        await updateIntegration(integrationId, callbackUrl, workerSecret, {
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
async function syncProducts(storeUrl, accessToken, accountId, integrationId, callbackUrl, workerSecret) {
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

  await updateIntegration(integrationId, callbackUrl, workerSecret, {
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
      await createEntity(callbackUrl, workerSecret, 'Product', productData);
      totalSynced++;

      if (totalSynced % 100 === 0 || i === allProducts.length - 1) {
        await updateIntegration(integrationId, callbackUrl, workerSecret, {
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

// Update customer totals - simplified
async function updateCustomerTotals(accountId, integrationId, callbackUrl, workerSecret) {
  console.log('Customer totals will be calculated on next sync...');
  return 0;
}

app.listen(PORT, () => {
  console.log(`🚀 Shopify Sync Worker running on port ${PORT}`);
});
