const { Worker, Queue } = require('bullmq');
const fs = require('fs').promises;
const path = require('path');

const pdfParse = require('pdf-parse');
const { GoogleGenerativeAI } = require("@google/generative-ai");
const { Pinecone } = require('@pinecone-database/pinecone');

require('dotenv').config();

const pinecone = new Pinecone({
    "apiKey": process.env.PINECONE_API_KEY
});

const index = pinecone.Index('pdpindex');

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
const model = genAI.getGenerativeModel({ model: "text-embedding-004"});

// Redis connection
const connection = {
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT,
};

const queue = new Queue('pdf_transform', { connection });

const testWorker = new Worker(
  'pdf_transform',
  async (job) => {
    await job.log(`Processing job ${job.id}`);
    await job.log(`read pdf file from: ${job.data.pdf_path}`);
    await ingestPdf(job, job.data.pdf_path, job.data.pdf_path_dest);
  },
  { connection }
);

async function ingestPdf(job, srcFolder, destFolder) {
    try {
      // Create destination folder if not exists
      await fs.mkdir(destFolder, { recursive: true });
  
      const files = await fs.readdir(srcFolder);
    
      for (const file of files) {
        const srcPath = path.join(srcFolder, file);
        const destPath = path.join(destFolder, file);
  
        const stats = await fs.stat(srcPath);
        await job.log(`starting to embed pdf file: ${file}`);
        if (stats.isFile() && path.extname(file).toLowerCase() === '.pdf') {
          // Read the PDF file
          const pdfData = await fs.readFile(srcPath);

          await job.log(`starting to extract text from pdf file: ${file}`);
          const pdfText = await extractTextFromPdf(pdfData);
          await job.log(`extracted text from pdf file: ${pdfText}`);
          // Create embeddings for each page (or the whole document)
          await job.log(`embeddings text`);
          const embeddings = await createEmbeddings(pdfText);
          await job.log(`text embeded`);
          // Index embeddings in Pinecone
          await job.log(`start indexing to pinecone`);
          await indexEmbeddings(file, embeddings);
          await job.log(`indexed in pinecone`);
          // Move the PDF file after processing
          await fs.rename(srcPath, destPath);
          await job.log(`Moved: ${file} -> ${destFolder}`);
        } else {
          await job.log(`Skipped: ${file} (Not a PDF)`);
        }
      }
  
      await job.log('All PDF files have been processed and moved successfully.');
    } catch (err) {
      await job.log(`Error processing files: ${err.message}`);
      throw err;
    }
}

async function extractTextFromPdf(pdfData) {
    try {
      const pdfParsed = await pdfParse(pdfData);
      return pdfParsed.text; // Return the text content of the entire PDF
    } catch (err) {
      throw new Error(`Error extracting text from PDF: ${err.message}`);
    }
}

function splitTextIntoChunks(text, maxChunkSize) {
    const chunks = [];
    let startIndex = 0;

    while (startIndex < text.length) {
        const endIndex = Math.min(startIndex + maxChunkSize, text.length);
        chunks.push(text.substring(startIndex, endIndex));
        startIndex = endIndex;
    }

    return chunks;
}

async function createEmbeddings(text) {
    try {
        const maxChunkSize = 8000; // Maximum characters per chunk (leave room for safety)
        const chunks = splitTextIntoChunks(text, maxChunkSize);

        let allEmbeddings = [];
        for (const chunk of chunks) {
            // Generate embeddings for each chunk
            const result = await model.embedContent(chunk);
            allEmbeddings.push(result.embedding.values);
        }

        return allEmbeddings; // Array of embeddings for each chunk
    } catch (err) {
        throw new Error(`Error generating embeddings: ${err.message}`);
    }
}

async function indexEmbeddings(fileName, embeddings) {
    try {
      // Index each page's embeddings (or the whole document)
      const upsertData = embeddings.map((embedding, index) => ({
        id: `${fileName}-${index}`, // Unique ID for each embedding
        values: embedding,
        metadata: { file: fileName, page: index },
      }));
  
      // Upsert the embeddings to Pinecone
      const upsertResponse = await index.namespace("ns1").upsert(upsertData);
      console.log(`Successfully indexed ${upsertData.length} vectors for ${fileName}`);
    } catch (err) {
      throw new Error(`Error indexing embeddings in Pinecone: ${err.message}`);
    }
}

queue.add('pdf_transform_job', {
    pdf_path: './pdf',
    pdf_path_dest: './archieve_pdf'
  }, {
    // repeat: {
    //   cron: '* * * * *' // Cron expression for every day at midnight
    // }
    delay: 60000,
    attempts: 1,
});

testWorker.on('completed', (job) => {
  console.log(`Job completed with ID: ${job.id}`);
});

testWorker.on('failed', (job, err) => {
  console.error(`Job failed with ID: ${job.id}`, err);
});
