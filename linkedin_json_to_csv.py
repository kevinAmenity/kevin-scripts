import pandas as pd
import argparse
import json
import csv
import re


ROOT_DICTS = ['person', 'email_company', 'analysis']
FORBIDDEN_KEYS = ['snapshots']


def read_as_str(document):
    
    """
    Reads in the raw .json file 
    as plain text before it is 
    cleaned.
    """
    
    with open(document, 'r', encoding='utf-8') as f:
        return f.read()
    
    
def clean_json_text(text):
    
    """
    Cleans the plain text in order 
    to be read correctly by the 
    JSON module.
    """
    
    regexp = re.compile(r'(\{|,|"|[0-9a-z]|\}|\[|\])[\t\s]+(\}|\{|\]|,|")')
    subst = r'\1\2'
    while re.search(regexp, text):
        text = re.sub(regexp, subst, text)
    return text.replace('}},"results":', '},"results":')


def write_json(new_document, text):
    
    """
    Writes the now-cleaned plain 
    text to file.
    """
    
    with open(new_document, 'w', encoding='utf-8') as f:
        f.write(text)
    
    
def read_json(document):
    
    """
    Reads the cleaned JSON file.
    """
    
    data = []
    for line in open(document, 'r', encoding='utf-8'):
        data.append(json.loads(line))
    return data

    
def get_lowest_leaf(target_dict, current_dict):
    
    """
    Gets the lowest dictionary value 
    recursively.
    """
    
    for k, v in target_dict.items():
        if isinstance(v, dict):
            current_dict = get_lowest_leaf(v, current_dict)
        elif isinstance(v, list):
            try:
                v = '; '.join(v)
            except TypeError:
                # The list is a list of dicts, not strs
                continue
        else:
            if k in ROOT_DICTS or k in FORBIDDEN_KEYS or not v: 
                continue
            k = k.replace('{', '').replace('}', '')
            current_dict.update({k: v})
    return current_dict

    
def write_csv(df, document):
    
    """
    Writes final dataframe to 
    CSV. 
    """
    
    df.to_csv(document, index=False, quoting=csv.QUOTE_ALL)
    
    
if __name__ == '__main__':
    args = argparse.ArgumentParser(description='Converts LinkedIn JSON data into CSV format.')
    args.add_argument('--json', required=True, help='The input JSON file.')
    params = args.parse_args()
    
    json_file = params.json
    clean_json = f'{json_file[:-5]}_clean.json'
    final_csv = f'{json_file[:-5]}_reformatted.csv'
    
    json_text = read_as_str(json_file)
    json_text = clean_json_text(json_text)
    write_json(clean_json, json_text)
    json_data = read_json(clean_json)

    if not len(json_data) or 'results' not in json_data[0]:
        raise Exception('UserError: Something has gone wrong '
                        'with the JSON structure.')
        
    final_df = pd.DataFrame()
    for user, user_data in json_data[0]['results'].items():
        print(f'Grabbing data for {user}...')

        analysis_data = {'email': user}
        if 'analysis' in user_data and user_data['analysis']:
            analysis_data.update(get_lowest_leaf(user_data['analysis'], analysis_data))

        person_data = dict()            
        if 'person' in user_data and user_data['person']:
            person_data = get_lowest_leaf(user_data['person'], person_data)
            
        linkedin_data = dict()
        if 'email_company' in user_data and user_data['email_company']:
            linkedin_data = get_lowest_leaf(user_data['email_company'], linkedin_data)

        all_data = dict(list(analysis_data.items()) + 
                        list(person_data.items()) + 
                        list(linkedin_data.items()))
        all_df = pd.DataFrame(all_data, index=[0])
        
        final_df = pd.concat([final_df, all_df], axis=0, sort=False)
    
    print('Writing final CSV...')
    write_csv(final_df, final_csv)
