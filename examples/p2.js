const BigDataReceiver = require('../index').BigDataReceiver;
const bdr = new BigDataReceiver();

bdr.receiveData(() => {console.log('Data is ready...');});

