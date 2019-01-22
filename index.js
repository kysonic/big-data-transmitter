const fs = require('fs');
const asyncFs = fs.promises;
const bigJson = require('big-json');
const nats = require('nats');
const path = require('path');

const CHUNK_SIZE = 10;

class NatsProvider {
    constructor(connecition = '0.0.0.0:4222', room = 'room1') {
        this.nc = nats.connect(connecition);
        this.room = room;
        this.binds();
    }

    binds() {
        this.publishData = this.publishData.bind(this);
        this.subscribeData = this.subscribeData.bind(this);
        this.publishEnd = this.publishEnd.bind(this);
        this.subscribeEnd = this.subscribeEnd.bind(this);
        this.publishStart = this.publishStart.bind(this);
        this.subscribeStart = this.subscribeStart.bind(this);
    }

    publishData(chunk) {
        this.nc.publish(`data[${this.room}]`, chunk.toString('utf8'));
    }

    subscribeData(cb) {
        this.nc.subscribe(`data[${this.room}]`, cb);
    }

    publishEnd() {
        this.nc.publish(`end[${this.room}]`);
    }

    subscribeEnd(cb) {
        this.nc.subscribe(`end[${this.room}]`, cb);
    }

    publishStart() {
        this.nc.publish(`start[${this.room}]`);
    }

    subscribeStart(cb) {
        this.nc.subscribe(`start[${this.room}]`, cb);
    }
}

class BigDataTransmitter extends NatsProvider {
    async transferData(object) {
        if (!object) {
            throw new Error('Setup some object: object, array, or file path.');
        }

        if(typeof object === 'string') {
            try {
                const stat = await asyncFs.lstat(object);
                if(stat.isFile()) {
                    return this.transferFile(object);
                }
            }
            catch (e) {console.log('It is not a file...');}
        }

        if(typeof object === 'object') {
            return this.transferObject(object);
        }

        return this.transferString(object.toString());
    }

    isJSON(filePath) {
        return path.extname(filePath) === 'json';
    }

    transferFile(filePath) {
        const readStream = fs.createReadStream(filePath);
        if(this.isJSON(filePath)) {
            const parseStream = bigJson.createParseStream();
            readStream.pipe(parseStream);
        }
        this.publishStart();
        readStream.on('data', this.publishData);
        readStream.on('end', this.publishEnd);
    };

    transferObject(object) {
        this.publishStart();
        const stringifyStream = bigJson.createStringifyStream({
            body: object
        });
        stringifyStream.on('data', this.publishData);
        stringifyStream.on('end', this.publishEnd);
    }

    transferString(string) {
        this.publishStart();
        while (string.length > 0) {
            this.publishData(string.substring(0, CHUNK_SIZE));
            string = string.substring(CHUNK_SIZE);
        }
        this.publishEnd();
    }
}

class BigDataReceiver extends NatsProvider {
    constructor(connection, room, filePath = './tmp') {
        super();
        this.filePath = filePath;
        this.ws = fs.createWriteStream(this.filePath);

        this.onData = this.onData.bind(this);
        this.eraseFile = this.eraseFile.bind(this);
    }

    eraseFile() {
        fs.writeFile(this.filePath, '');
    }

    receiveData(cb) {
        this.subscribeStart(this.eraseFile);
        this.subscribeData(this.onData);
        this.subscribeEnd(cb);
    }

    onData(chunk) {
        this.ws.write(chunk);
    }
}

module.exports.BigDataTransmitter = BigDataTransmitter;
module.exports.BigDataReceiver = BigDataReceiver;
