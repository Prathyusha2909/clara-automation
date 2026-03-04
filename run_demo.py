import os
import json

def extract_demo_data(text, account_id):
    data =  {
    "account_id": account_id,
    "company_name": "",
    "business_hours": "",
    "office_address": "",
    "services_supported": [],
    "emergency_definition": [],
    "emergency_routing_rules": "",
    "non_emergency_routing_rules": "",
    "call_transfer_rules": "",
    "integration_constraints": "",
    "after_hours_flow_summary": "",
    "office_hours_flow_summary": "",
    "questions_or_unknowns": [],
    "notes": ""
}
        
    


    # Extract company name
    if "We are" in text:
        start = text.find("We are") + len("We are ")
        end = text.find(".", start)
        data["company_name"] = text[start:end].strip()
    else:
        data["questions_or_unknowns"].append("Company name missing")

    # Extract business hours
    if "Monday to Friday" in text:
        data["business_hours"] = {
            "days": "Monday-Friday",
            "start": "08:00",
            "end": "16:00",
            "timezone": "EST"
        }
    else:
        data["questions_or_unknowns"].append("Business hours missing")

    # Extract emergency types
    if "sprinkler" in text.lower():
        data["emergency_definition"].append("sprinkler leak")

    if "fire alarm" in text.lower():
        data["emergency_definition"].append("fire alarm triggered")

    return data


def save_v1(account_id, memo):
    path = f"outputs/accounts/{account_id}/v1"
    os.makedirs(path, exist_ok=True)

    with open(f"{path}/memo.json", "w") as f:
        json.dump(memo, f, indent=4)

def generate_agent_spec(memo, version):
    return {
        "agent_name": memo["company_name"] + " Voice Agent",
        "version": version,
        "business_hours": memo["business_hours"],
        "emergency_definition": memo["emergency_definition"],
        "system_prompt": f"""
You are Clara, the voice assistant for {memo['company_name']}.

BUSINESS HOURS:
{memo['business_hours']}

BUSINESS HOURS FLOW:
1. Greet caller
2. Ask purpose
3. Collect name and number
4. Transfer appropriately
5. If transfer fails, apologize and assure follow-up
6. Ask if anything else
7. Close

AFTER HOURS FLOW:
1. Greet caller
2. Ask purpose
3. Confirm if emergency
4. If emergency, collect name, number, address
5. Attempt transfer
6. If transfer fails, assure callback
7. If non-emergency, collect details
8. Close
"""
    }
# MAIN
import os

demo_folder = "inputs/demo"

for file in os.listdir(demo_folder):

    if file.endswith(".txt"):

        account_id = file.replace(".txt", "")

        with open(f"{demo_folder}/{file}") as f:
            transcript = f.read()

        memo = extract_demo_data(transcript, account_id)
        agent_spec = generate_agent_spec(memo, "v1")

        save_v1(account_id, memo)

        with open(f"outputs/accounts/{account_id}/v1/agent_spec.json", "w") as f:
            json.dump(agent_spec, f, indent=4)

        print(f"{account_id} v1 created successfully!")