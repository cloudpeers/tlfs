import init, { LocalFirst } from 'local-first';

async function run() {
  await init();

  const sdk = await LocalFirst.init();
  (window as any).sdk = sdk;
  console.log("result", sdk);
}
run();
