# 1. Seamless Journal

Real-time Journal for Notion

The Seamless Journal provides near-real-time tracking of positions entered and exited on various exchange platforms and subsequently updates a Notion database.

---

- [1. Seamless Journal](#1-seamless-journal)
- [2. Documentation](#2-documentation)
  - [2.1. Process](#21-process)
    - [2.1.1. Main process](#211-main-process)
    - [2.1.2. Websocket Bybit](#212-websocket-bybit)

---

**TODO**:
- Integration
   1. ~~Bybit~~
   2. Coinbase
   3. Oanda
   4. Binance
- Combine `websockets` and deploy on a server


# 2. Documentation
TODO:
- add requirements
- add environment

## 2.1. Process
The process currently caputres entering positions into a Notion database and checks if a trade has already been logged on the remote database.

### 2.1.1. Main process

```mermaid
graph LR; 
A("websocket: bybit");
B("websocket: coinbase");
C["notion db"];

A -.-> C;
B -.-> C;

```
### 2.1.2. Websocket Bybit

```mermaid

graph TD; 
A("check if in position");
Y1("yes");
Y12("check if already logged");

Y21("yes");
Y22("do nothing");

Y31("no");
Y32("log the trade into the file");
Y33("send request to notion db");

N2["No"];
N21["do nothing"];

A -.-> Y1 -.-> Y12 --> Y21 --> Y22;
A -.-> Y1 -.-> Y31 --> Y32 --> Y33;

A -.-> N2 -.-> N21;

```
