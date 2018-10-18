import requests

data = requests.get('https://tools.wmflabs.org/corhist/stats').json()

types = {
    'Type': [''],
    'Value type': [''],
    'One-of': ['Q21510859'],
    'Item requires stm.': [''],
    'Value requires stm.': ['Q21510864'],
    'Conflict with': ['Q21502838'],
    'Inverse/Symmetric': ['Q21510855', 'Q21510862'],
    'Single value': ['Q19474404'],
    'Distinct values': ['Q21502410'],
    'All': list(data.keys())
}

users = set()

for type, type_ids in types.items():
    result = {}
    for type_id in type_ids:
        for key, value in data.get(type_id, {}).items():
            result[key] = result.get(key, 0) + value.get('*', 0)
            users.update(value.keys())
    all = result.get('PROPOSED', 0) + result.get('APPROVED', 0) + result.get('REJECTED', 0) + result.get('OBSOLETE', 0)
    ratio = round(result.get('APPROVED', 0) / (result.get('APPROVED', 0) + result.get('REJECTED', 0)), 2) \
        if (result.get('APPROVED', 0) + result.get('REJECTED', 0)) != 0 \
        else ''
    print('{} & {} & {} & {} & {} & {} \\\\'.format(
        type, all, result.get('APPROVED', 0), result.get('REJECTED', 0), result.get('OBSOLETE', 0), ratio)
    )

users.remove('*')
print('User: {}'.format(len(users)))
