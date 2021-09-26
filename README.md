# The Local-First SDK

[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](https://github.com/cloudpeers/tlfs)
[![dependency status](https://deps.rs/repo/github/cloudpeers/tlfs/status.svg?style=flat-square)](https://deps.rs/repo/github/cloudpeers/tlfs)

[![PRs welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square)](#contributing)
![commits](https://img.shields.io/github/commit-activity/m/cloudpeers/tlfs?style=flat-square)
![contributors](https://img.shields.io/github/contributors/cloudpeers/tlfs?style=flat-square)


## Philosophy

> There is no cloud, it's just someone else's computer.

The Local-First SDK offers a stack to write applications as productively as when
using state-of-the-art cloud-based architectures, while providing the Seven
Ideals for Local-First Software [0] -- basically for free:
* Software can respond near-instantaneously to user input.
  _(No waiting on server round-trips, no spinners.)_
* Cross-device synchronization.
  _(Pick up work on your mobile device just where you left off with your laptop.)_
* "Offline-First" as a subset of Local-First.
  _(Connectivity is irrelevant when interacting with the application>0_
* Seamless collaboration with other peers.
  _(Edit and sync shared data without fear of conflicts.)_
* Full data agency.
  _(Do what you want with your data, it's yours only.)_
* Secure and private data management.
  _(Everything is encrypted, only you have the keys.)_
* Full ownership and control over the application's data.
  _(No one can take away a service from you.)_

[0]: https://martin.kleppmann.com/papers/local-first.pdf

--------

## Components

The Local-First SDK comprises the following components:
1. User and Access Control:
   ... (key management, acl)
1. Multi-Device Support and Collaboration:
   ... (device auth, p2p, peer discovery (mdns and via cloud peer))
1. Data Persistence
   ... (cloud peer or self-hosted)
1. Multi device support and interoperability
   ... (browser, native, android/ios?)

--------

## Artifacts

The Local-First SDK comes in three flavours:
1. An opinionated Javascript package (with Typescript bindings) to write
   Local-First applications targeting the browser.
1. A library which can be embedded into other applications, either as a rust
   library or a C-compatible FFI.
1. A native, permanent process shepherding the user's data. Applications can
   interface with this daemon via HTTP.

### The Local-First Javascript SDK

As the browser's API guarantees are weak, its environment has to be considered
ephemeral[^1]. This is why the optional Cloud-Peer supplemental services
complement the browser environment very well (data persistence, peer discovery).

As of now, the SDK is just offered as an ES module, requiring asynchronous
import:
```js
import * as localFirst from 'local-first';

await localFirst.init();
```

[..]

[^1]: Most notably this is about persistence of user data (key material and
application data). However, it's easy to lose one's browsing data by switching
to another browser profile/container, etc.


--------

## Under the hood

Rust, libp2p, crdts, cambria, .. 
--> INSERT AWESOMENESS HERE <--

--------

## License

Licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contributing
Be respectful. Check out our [Contribution Guidelines](./CONTRIBUTING.md) for
specifics.
Any contribution intentionally submitted for inclusion in the work by you, as
defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.
