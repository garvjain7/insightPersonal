import json
with open('lint_results.json', encoding='utf-16') as f:
    data = json.load(f)
    for file in data:
        if len(file['messages']) > 0:
            print(f"{file['filePath']}: {len(file['messages'])} issues")
            for msg in file['messages'][:5]:
                print(f"  Line {msg['line']}: {msg['message']} ({msg.get('ruleId', 'N/A')})")
            if len(file['messages']) > 5:
                print("  ...")
