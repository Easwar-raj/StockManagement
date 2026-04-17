const express = require("express");
const { Pool } = require("pg");
const app = express();

const pool = new Pool({
  user: 'postgres',
  host: 'localhost',
  database: 'store',
  password: 'postgres',
  port: 5432,
});

app.use(express.json());

// Create tables if not exist
async function initDB() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS products (
        sku VARCHAR PRIMARY KEY,
        name VARCHAR NOT NULL,
        stock_quantity INTEGER NOT NULL CHECK (stock_quantity >= 0)
      );
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS orders (
        id SERIAL PRIMARY KEY,
        idempotency_key VARCHAR UNIQUE NOT NULL,
        status VARCHAR NOT NULL DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS order_items (
        order_id INTEGER REFERENCES orders(id),
        sku VARCHAR REFERENCES products(sku),
        quantity INTEGER NOT NULL,
        PRIMARY KEY (order_id, sku)
      );
    `);
    console.log("Database initialized");
  } catch (err) {
    console.error("Error initializing database", err);
  }
}

initDB();

// Routes

// POST /products
app.post('/products', async (req, res) => {
  const { sku, name, stock_quantity } = req.body;
  if (!sku || !name || stock_quantity === undefined) {
    return res.status(400).json({ error: 'Missing required fields' });
  }
  try {
    const result = await pool.query(
      'INSERT INTO products (sku, name, stock_quantity) VALUES ($1, $2, $3) RETURNING *',
      [sku, name, stock_quantity]
    );
    res.status(201).json(result.rows[0]);
  } catch (err) {
    if (err.code === '23505') { // unique violation
      res.status(409).json({ error: 'Product SKU already exists' });
    } else {
      res.status(500).json({ error: 'Internal server error' });
    }
  }
});

// GET /products/:sku
app.get('/products/:sku', async (req, res) => {
  const { sku } = req.params;
  try {
    const result = await pool.query('SELECT * FROM products WHERE sku = $1', [sku]);
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Product not found' });
    }
    res.json(result.rows[0]);
  } catch (err) {
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /orders
app.post('/orders', async (req, res) => {
  const { idempotency_key, items } = req.body;
  if (!idempotency_key || !items || !Array.isArray(items)) {
    return res.status(400).json({ error: 'Invalid request' });
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // Check for existing order with idempotency_key
    const existingOrder = await client.query('SELECT id FROM orders WHERE idempotency_key = $1', [idempotency_key]);
    if (existingOrder.rows.length > 0) {
      await client.query('ROLLBACK');
      return res.status(200).json({ message: 'Order already exists', order_id: existingOrder.rows[0].id });
    }

    // Check stock for all items
    for (const item of items) {
      const { sku, quantity } = item;
      const product = await client.query('SELECT stock_quantity FROM products WHERE sku = $1 FOR UPDATE', [sku]);
      if (product.rows.length === 0) {
        await client.query('ROLLBACK');
        return res.status(400).json({ error: `Product ${sku} not found` });
      }
      if (product.rows[0].stock_quantity < quantity) {
        await client.query('ROLLBACK');
        return res.status(400).json({ error: `Insufficient stock for ${sku}` });
      }
    }

    // Create order
    const orderResult = await client.query(
      'INSERT INTO orders (idempotency_key, status) VALUES ($1, $2) RETURNING id',
      [idempotency_key, 'completed']
    );
    const orderId = orderResult.rows[0].id;

    // Insert order items and update stock
    for (const item of items) {
      const { sku, quantity } = item;
      await client.query(
        'INSERT INTO order_items (order_id, sku, quantity) VALUES ($1, $2, $3)',
        [orderId, sku, quantity]
      );
      await client.query(
        'UPDATE products SET stock_quantity = stock_quantity - $1 WHERE sku = $2',
        [quantity, sku]
      );
    }

    await client.query('COMMIT');
    res.status(201).json({ order_id: orderId });
  } catch (err) {
    await client.query('ROLLBACK');
    if (err.code === '23505') { // unique violation on idempotency_key
      res.status(200).json({ message: 'Order already exists' });
    } else {
      res.status(500).json({ error: 'Internal server error' });
    }
  } finally {
    client.release();
  }
});

// GET /orders/:id
app.get('/orders/:id', async (req, res) => {
  const { id } = req.params;
  try {
    const orderResult = await pool.query('SELECT * FROM orders WHERE id = $1', [id]);
    if (orderResult.rows.length === 0) {
      return res.status(404).json({ error: 'Order not found' });
    }
    const order = orderResult.rows[0];

    const itemsResult = await pool.query('SELECT * FROM order_items WHERE order_id = $1', [id]);
    order.items = itemsResult.rows;

    res.json(order);
  } catch (err) {
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.listen(3000, () => {
  console.log("Server running on port 3000");
});