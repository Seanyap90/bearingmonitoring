const http = require('http');
const request = require('request');
const fs = require('fs');
const aedes = require('aedes')();
const serverIP = 'localhost'; // Replace with your server's IP address
const clientPort = 5001;
const mqttServer = require('net').createServer(aedes.handle);
const port = 1883;

let client = null
let dataQueue = []; // Queue to store data for POST requests

mqttServer.listen(port, function () {
  console.log('Aedes MQTT server started and listening on port ', port)
})

aedes.on('client', function(client) {
    console.log('Client Connected:', client.id);
});

aedes.on('publish', function(packet, client) {
    if (packet.topic === 'bearing/sendData') {
        let body = packet.payload.toString();
        console.log(`Received publish on topic ${packet.topic}`);
        try {
            const parsedData = JSON.parse(body);
            const firstRow = parsedData[0];
            const { h, m, s, Directory } = firstRow;
            const formattedTime = `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
            
            console.log(formattedTime + " " + Directory);

            // Add data to the queue for processing
            dataQueue.push(parsedData);

            // If queue was empty before, start processing
            if (dataQueue.length === 1) {
                processQueue();
            }
        } catch (e) {
            console.error('Failed to parse message:', e);
        }
    } else if (packet.topic === 'bearing/label') {
        let body = packet.payload.toString();
        console.log(`Received publish on topic ${packet.topic}`);
        try {
            const parsedData = JSON.parse(body);
        
            console.log(body);
    
            // Add data to the queue for processing
            dataQueue.push(parsedData);
            console.log("queue length: " + dataQueue.length)

            // If queue was empty before, start processing
            if (dataQueue.length === 1) {
                processQueue();
            }
        } catch (e) {
            console.error('Failed to parse message:', e);
        }
    }
});

function sendToClient(data) {
    if (client) {
        client.write(`data: ${JSON.stringify(data)}\n\n`);
    }
}

async function processQueue() {
    while (dataQueue.length > 0) {
        const data = dataQueue.shift(); // Take data from the queue
        try {
            // Check if data is an array and truncate if necessary
            if (Array.isArray(data) && data.length > 0 && data[0].h !== undefined) {
                // Process array of rows with timestamps
                const slicedData = data.slice(0, 20); // Take only the first 20 rows

                for (const rowData of slicedData) {
                    const hours = String(rowData.h).padStart(2, '0');
                    const minutes = String(rowData.m).padStart(2, '0');
                    const seconds = String(rowData.s).padStart(2, '0');
                    const milliseconds = String(rowData.ms).padStart(3, '0');
                    const timeString = `${hours}:${minutes}:${seconds}.${milliseconds}`;

                    const formattedData = {
                        Hacc: rowData.Hacc,
                        Vacc: rowData.Vacc,
                        Directory: rowData.Directory,
                        Time: timeString,
                    };

                    sendToClient(formattedData);
                }
            } else {
                // Handle single-row data without modification
                console.log("test")
                sendToClient(data);
            }
        } catch (error) {
            console.error('Error processing data:', error);
        }

        //sendToClients(data);
        //console.log("sent")
        
    }
}

http.createServer((req, res) => {
    res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Access-Control-Allow-Origin': '*'
    });
    res.write('\n');
    // Allow the client to connect to this server for events
    client = res;
    console.log('Client connected:', req.socket.remoteAddress + ':' + req.socket.remotePort);

    req.on('close', () => {
        client = null
        console.log('Client disconnected');
    });
}).listen(clientPort, "<your server ip>", () => {
  console.log(`SSE server running at http://<your server ip>:${clientPort}`);
});
