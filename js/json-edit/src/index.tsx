import * as React from 'react'
import { render } from 'react-dom'
// import "./index.css"
import init, { LocalFirst } from 'local-first'
import { App } from './app'

let sdk: LocalFirst
async function run() {
  await init()

  const tlfs = await LocalFirst.init();
  (window as any).sdk = tlfs
  sdk = tlfs
  console.log('result', sdk)
}
run().then(async () => {
  const doc = sdk.createDoc('todoapp')
  console.log('Doc ID', doc.id())
  doc.subscribe('', async doc => {
    const addresses = await sdk.addresses()

    render(<App doc={doc} onChange={console.log} peerId={sdk.peerId()} ownAddresses={addresses} />, document.getElementById('app'))
  })
})
