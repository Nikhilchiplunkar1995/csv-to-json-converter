const express = require('express');
const csvtojson = require('csvtojson');
const { Pool } = require('pg');
const multer = require('multer');
const dotenv = require('dotenv');

dotenv.config();

const app = express();
const port = process.env.PORT || 3000;
const upload = multer({ dest: 'uploads/' });

const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_DATABASE,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT,
});

app.use(express.json());

app.post('/convert-and-upload', upload.single('csvFile'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No CSV file uploaded' });
    }

    const jsonArray = await csvtojson().fromFile(req.file.path);

    const client = await pool.connect();
    await client.query('BEGIN');

    try {
      const insertQuery =
        'INSERT INTO your_table_name (first_name, last_name, age, line1, line2, city, state, gender) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)';

      for (const record of jsonArray.slice(1)) {
        await client.query(insertQuery, [
          record['name.firstName'],
          record['name.lastName'],
          parseInt(record.age),
          record['address.line1'],
          record['address.line2'],
          record['address.city'],
          record['address.state'],
          record.gender,
        ]);
      }

      await client.query('COMMIT');
      res.status(200).json({ message: 'Data successfully converted and uploaded to PostgreSQL.' });
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
