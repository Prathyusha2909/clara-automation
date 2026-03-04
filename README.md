# Clara AI - Voice Agent Configuration Automation

## Overview

This project automates the process of generating and updating AI voice agent configurations based on customer call transcripts.

The system processes two types of transcripts:

1. Demo Call – Used to create the initial agent configuration (v1)
2. Onboarding Call – Used to update the existing configuration and create a new version (v2)

The project ensures that updates are version-controlled and that changes are logged clearly.

---

## How It Works

### Step 1: Demo Processing (run_demo.py)

- Reads transcript from inputs/demo
- Extracts structured information (company name, business hours, emergency definition)
- Saves structured memo as v1
- Generates agent_spec.json for the voice agent

Output:
outputs/accounts/<account_id>/v1/

---

### Step 2: Onboarding Update (run_onboarding.py)

- Loads existing v1 configuration
- Reads onboarding transcript
- Updates only modified fields
- Saves updated configuration as v2
- Generates updated agent_spec.json
- Creates changes.json to track modifications

Output:
outputs/accounts/<account_id>/v2/

---

## Key Features

- Version-controlled configuration management
- Deterministic rule-based updates (no hallucination)
- Change tracking with old vs new values
- Clear folder structure for each account
- Simple and extensible design

---

## Folder Structure

clara-automation/
│
├── inputs/
│   ├── demo/
│   └── onboarding/
│
├── outputs/
│   └── accounts/
│
├── run_demo.py
├── run_onboarding.py
└── README.md

---

## Future Improvements

- Support for multiple accounts in batch mode
- More advanced transcript parsing logic
- Validation layer for missing fields
- Automated testing

---

## How To Run

1. Run demo processing:
   python run_demo.py

2. Run onboarding update:
   python run_onboarding.py
   