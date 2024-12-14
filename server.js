const Arena = require('bull-arena');
const { Queue } = require('bullmq');
const express = require('express');
const basicAuth = require('express-basic-auth');
require('dotenv').config();

// Redis connection options
const connection = {
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT,
};

// Arena configuration
const arenaConfig = Arena(
  {
    BullMQ: Queue, // Specify BullMQ integration
    queues: [
      {
        // Name of the queue
        name: 'pdf_transform',
        hostId: 'pdf_transform_host',
        type: 'bullmq', // Explicitly set the type to BullMQ
        redis: connection,
      },
    ],
  },
  {
    // Use an Express adapter
    basePath: '/arena',
    disableListen: true,
  }
);

const app = express();

// Apply basic authentication middleware to the /arena route
app.use('/arena', basicAuth({
  users: { 'admin': process.env.ARENA_ADMIN_PASSWORD }, // Replace with your username and password
  challenge: true,  // This will prompt the browser for login details
}));

// Mount the Arena dashboard
app.use('/', arenaConfig);

app.listen(3000, () => {
  console.log('Arena is running on http://localhost:3000/arena');
});