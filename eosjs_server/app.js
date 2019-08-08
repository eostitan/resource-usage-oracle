const express = require('express')
const app = express()
const port = 3000

const { Api, JsonRpc, RpcError } = require('eosjs');
const { JsSignatureProvider } = require('eosjs/dist/eosjs-jssig');  // development only
const fetch = require('node-fetch');
const { TextEncoder, TextDecoder } = require('util');
const key = process.env.SUBMISSION_PRIVATE_KEY;
const signatureProvider = new JsSignatureProvider([key]);
const rpc = new JsonRpc(process.env.EOSIO_API_NODE_1, { fetch });
const api = new Api({ rpc, signatureProvider, textDecoder: new TextDecoder(), textEncoder: new TextEncoder() });

app.use(express.json());

app.get('/', (req, res) => {
  res.send('Server up!')
})

app.post('/push_transaction', async (req, res) => {
  var tx = req.body;
  try {
    const result = await api.transact(tx, {'blocksBehind': 3, 'expireSeconds': 30});
    res.send(result)
  } catch (e) {
    res.send({"error": "" + e})
  }
})

app.listen(port, () => console.log(`EOSJS server listening on port ${port}!`))
