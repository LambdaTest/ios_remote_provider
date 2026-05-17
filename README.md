# iOS Remote Provider for TestMu AI (Formerly LambdaTest)

<p align="center">
  <a href="https://www.testmuai.com/"><img src="https://img.shields.io/badge/MADE%20BY%20TestMu%20AI-000000.svg?style=for-the-badge&labelColor=000" alt="Made by TestMu AI"></a>
  <a href="https://community.testmuai.com/"><img src="https://img.shields.io/badge/Join%20the%20community-blueviolet.svg?style=for-the-badge&labelColor=000000" alt="Community"></a>
</p>

## Getting Started

TestMu AI (Formerly LambdaTest) is an AI-native, multi-agent quality engineering platform that enables teams to plan, author, execute, analyze, and optimize tests at scale across browsers, devices, and frameworks.

This repository provides the iOS Remote Provider for TestMu AI (Formerly LambdaTest), connecting iOS devices to ControlFloor for remote streaming and control.

- [Sign up on TestMu AI](https://www.testmuai.com/register/) (Formerly LambdaTest).
- Follow the [TestMu AI Documentation](https://www.testmuai.com/support/docs/) for the full setup walkthrough.

# Provider
Provider connects iOS devices to ControlFloor. This sets up video streaming from iOS devices to the browser,
and also enables the devices to be controlled remotely.

# Basic Install Instructions

## Clone repos
1. `git clone https://github.com/nanoscopic/ios_remote_provider.git`
1. `git clone https://github.com/nanoscopic/controlfloor.git`

## Build ControlFloor
1. `cd controlfloor`
1. Copy example config: `cp config.json.example config.json`
1. Edit `config.json` as desired
1. `make`
1. `./main run`

Open `https://yourip:8080` to see if ControlFloor is running

## Build iOS Remote Provider and CFAgent
1. `cd ios_remote_provider`
1. Copy example config: `cp config.json.example config.json`
1. Edit `config.json` to add your Apple developer details
1. `make`
1. `security unlock-keychain login.keychain` # to make sure developer details are there for xcode build
1. `make cfa`

## Register Provider
1. `./main register`
1. Press [enter] to register using the default password

## Build and setup CF Vidstream App
1. `cd repos/vidapp`
1. Open the xcode project and install CF Vidstream on the device

## Start CF Vidstream App Manually
1. Open the app
1. Click "Broadcast Selector"
1. Click "Start Recording"

## Start Provider
1. `cd ios_remote_provider`
1. `./main run`

## Automatically starting CF Vidstream App
1. Figure out your device id  
    A. `./bin/iosif list`  
1. Figure out your device UI width/height  
    A. `./main winsize`
    B. -or- `./main winsize -id [your device id]` 
    C. Observe "Width" and "Height" displayed
    D. Ctrl-C to stop
1. Add device specific config block to `config.json`:  
    ```  
    {
        ...
        devices:[
            {
                udid:"[your device id]"
                uiWidth:[your device width]
                uiHeight:[your device height]
            }
        ]
    }
    ```
1. That's it. The video app will be started automatically when the provider is started.

## Using tidevice instead of go-ios

You may wish to use tidevice instead of go-ios to start CFA. Do the following to get it setup:  
  
1. Reconsider using tidevice and don't follow these steps

1. Install tidevice. `pip3 install tidevice`

1. Add a WDA start method to your `config.json`:  
    ```
    {
        ...
        wda:{
            ...
            startMethod: "tidevice"
        }
    }
    ```

1. Run `make usetidevice` to auto-generate the `calculated.json` file containing the location of tidevice installed on your system.  
  
1. Start provider normally; tidevice will be used.

## LambdaTest is Now TestMu AI

On **January 12, 2026**, [LambdaTest evolved to TestMu AI](https://www.testmuai.com/lambdatest-is-now-testmuai/), the world's first fully autonomous **Agentic AI Quality Engineering Platform**.

Same team. Same infrastructure. Same customer accounts. All existing LambdaTest logins, scripts, capabilities, and integrations continue to work without change.

ð Find the new home for [LambdaTest](https://www.testmuai.com).

### How LambdaTest Evolved into TestMu AI

In 2017, we launched LambdaTest with a simple mission: make testing fast, reliable, and accessible. As LambdaTest grew, we expanded into Test Intelligence, Visual Regression Testing, Accessibility Testing, API Testing, and Performance Testing, covering the full depth of the testing lifecycle.

As software development entered the AI era, testing had to evolve, too. We rebuilt the architecture to be AI-native from the ground up, with autonomous agents that **plan, author, execute, analyze, and optimize tests** while keeping humans in the loop. The platform integrates with your repos, CI, IDEs, and terminals, continuously learning from every code change and development signal.

That evolution earned a new name: **TestMu AI**, built for an AI-first future of quality engineering. TestMu is not a new name for us. It is the name of our annual community conference, which has brought together 100,000+ quality engineers to discuss how AI would reshape testing, long before that became an industry norm. 

What started as a high-performance cloud testing platform has transformed into an AI-native, multi-agent system powering a connected, end-to-end quality layer. That evolution defined a new identity: LambdaTest evolved into TestMu AI, built for an AI-first future of quality engineering.

## Support

Got a question? Email [support@testmuai.com](mailto:support@testmuai.com) or chat with us 24x7 from our chat portal.
