import os
import json

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

onboarding_folder = "inputs/onboarding"

for file in os.listdir(onboarding_folder):

    if file.endswith(".txt"):

        account_id = file.replace(".txt", "")

        # Load existing v1 memo
        with open(f"outputs/accounts/{account_id}/v1/memo.json") as f:
            memo = json.load(f)

        old_business_hours = memo["business_hours"]

        # Read onboarding transcript
        with open(f"{onboarding_folder}/{file}") as f:
            onboarding_text = f.read()

        # Update business hours if mentioned
        if "7am" in onboarding_text and "5pm" in onboarding_text:
            memo["business_hours"] = "Monday-Friday 7am-5pm EST"

        # Save v2 memo
        v2_path = f"outputs/accounts/{account_id}/v2"
        os.makedirs(v2_path, exist_ok=True)

        with open(f"{v2_path}/memo.json", "w") as f:
            json.dump(memo, f, indent=4)

        # Generate agent spec v2
        agent_spec = generate_agent_spec(memo, "v2")

        with open(f"{v2_path}/agent_spec.json", "w") as f:
            json.dump(agent_spec, f, indent=4)

        # Save change log
        changes = {
            "version": "v2",
            "changes": [
                {
                    "field": "business_hours",
                    "old_value": old_business_hours,
                    "new_value": memo["business_hours"],
                    "source": "onboarding transcript"
                }
            ]
        }

        with open(f"outputs/accounts/{account_id}/changes.json", "w") as f:
            json.dump(changes, f, indent=4)

        print(f"{account_id} v2 created successfully!")