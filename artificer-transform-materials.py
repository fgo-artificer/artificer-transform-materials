import boto3
import json

from jsonpath_ng import jsonpath
from jsonpath_ng.ext import parse

S3_BUCKET_NAME_EXTRACT = 'artificer-extract'
S3_BUCKET_NAME_TRANSFORM = 'artificer-transform'
FILE_NAME = 'materials.json'

MAP_COLUMNS_OLD_TO_NEW = {
    'title': 'TITLE',
    'uri': 'IMAGE_URL',
    'POSITION': 'POSITION'
}

LIST_TITLES_IN_ORDER = [
    'Gem of Saber',
    'Gem of Archer',
    'Gem of Lancer',
    'Gem of Rider',
    'Gem of Caster',
    'Gem of Assassin',
    'Gem of Berserker',
    'Magic Gem of Saber',
    'Magic Gem of Archer',
    'Magic Gem of Lancer',
    'Magic Gem of Rider',
    'Magic Gem of Caster',
    'Magic Gem of Assassin',
    'Magic Gem of Berserker',
    'Secret Gem of Saber',
    'Secret Gem of Archer',
    'Secret Gem of Lancer',
    'Secret Gem of Rider',
    'Secret Gem of Caster',
    'Secret Gem of Assassin',
    'Secret Gem of Berserker',
    'Proof of Hero',
    'Evil Bone',
    'Dragon Fang',
    'Void&#039;s Dust',
    'Fool&#039;s Chain',
    'Deadly Poison Needle',
    'Mystic Spinal Fluid',
    'Stake of Wailing Night',
    'Seed of Yggdrasil',
    'Ghost Lantern',
    'Octuplet Crystals',
    'Serpent Jewel',
    'Phoenix Feather',
    'Eternal Gear',
    'Forbidden Page',
    'Homunculus Baby',
    'Meteor Horseshoe',
    'Great Knight Medal',
    'Shell of Reminiscence',
    'Refined Magatama',
    'Claw of Chaos',
    'Heart of the Foreign God',
    'Dragon&#039;s Reverse Scale',
    'Spirit Root',
    'Warhorse&#039;s Young Horn',
    'Tearstone of Blood',
    'Black Beast Grease',
    'Lamp of Evil-Sealing',
    'Scarab of Wisdom',
    'Primordial Lanugo',
    'Cursed Beast Gallstone',
    'Mysterious Divine Wine',
    'Crystallized Lore',
    'Saber Piece',
    'Archer Piece',
    'Lancer Piece',
    'Rider Piece',
    'Caster Piece',
    'Assassin Piece',
    'Berserker Piece',
    'Saber Monument',
    'Archer Monument',
    'Lancer Monument',
    'Rider Monument',
    'Caster Monument',
    'Assassin Monument',
    'Berserker Monument',
    'Holy Grail'
]

bucket_extract = boto3.resource('s3').Bucket(S3_BUCKET_NAME_EXTRACT)
bucket_transform = boto3.resource('s3').Bucket(S3_BUCKET_NAME_TRANSFORM)

'''
download_json_from_s3

Download file to local filesystem, open file and return the contents as json
'''
def download_json_from_s3(remote_file_name):
    bucket_extract.download_file(Key=remote_file_name, Filename='/tmp/' + remote_file_name)
    f = open('/tmp/' + remote_file_name, 'r')
    return json.loads(f.readline())

'''
transform_materials

Do nothing for transform step lol
'''
def transform_materials(old_json):

    new_json = { 'data': [] }
    type_dicts = {}

    all_data_expr = parse('$.data[*]')
    for each_match in all_data_expr.find(old_json):
        each_entry = old_json['data'][each_match.id_pseudopath.right.index]

        # Skipping incomplete data entries
        if each_entry['title'] == '' or each_entry['uri'] == '' or each_entry['field_item_type'] == '':
            print('[transform] [materials] Anomaly data caught!')
            print('[transform] [materials] each_entry[{}]'.format(str(each_entry)))
            continue

        # Skipping unwanted typed data entries (QP is exception)
        if each_entry['field_item_type'] == 'Command Code Item' or \
            (each_entry['field_item_type'] == 'Consumable' and each_entry['title'] != 'QP') or \
            each_entry['field_item_type'] == 'Event Item' or \
            each_entry['field_item_type'] == 'Exchange' or \
            each_entry['field_item_type'] == 'Experience Up' or \
            each_entry['field_item_type'] == 'Other' or \
            each_entry['field_item_type'] == 'Points' or \
            each_entry['field_item_type'] == 'Status Up':
            continue

        try:
            position_num = LIST_TITLES_IN_ORDER.index(each_entry['title'])
            each_entry['POSITION'] = str(position_num+1)
        except:
            pass # swallow exception for O(1) perf?

        new_entry = {}
        #print('Printing entry: ' + str(each_entry))

        if each_entry['field_item_type'] not in type_dicts.keys():
            type_dicts[each_entry['field_item_type']] = []

        keys = len(each_entry.keys())
        #print('Printing keys: ' + str(keys))
        for i in range(0, keys):
            key = list(each_entry.keys())[i]
            #print('Printing key: ' + str(key))
            if key in MAP_COLUMNS_OLD_TO_NEW.keys():
                new_entry[MAP_COLUMNS_OLD_TO_NEW[key]] = each_entry[key]
        #print('Printing new_entry: ' + str(new_entry))

        type_dicts[each_entry['field_item_type']].append(new_entry)

    # flatten our dictionaries so that key and values are in same json level
    for each_item_type in type_dicts.keys():
        new_json['data'].append({ 'ITEM_TYPE': each_item_type, 'MATERIALS': type_dicts[each_item_type]})

    return new_json

'''
write_json_to_local

Opens a file, writes the json to the file, and closes it
'''
def write_json_to_local(json_obj, local_fpath):
    with open(local_fpath, "w+") as f:
        f.write(json.dumps(json_obj))
        f.close()

'''
upload_json_to_s3

Uploads the json file on the local file system to S3
'''
def upload_file_to_s3(local_file_name, remote_file_name):
    bucket_transform.upload_file(Filename=local_file_name, Key=remote_file_name)

'''
main

Driver function for local execution
'''
def main():
    json_materials = download_json_from_s3(FILE_NAME)
    json_materials = transform_materials(json_materials)
    write_json_to_local(json_materials, '/tmp/' + FILE_NAME)
    upload_file_to_s3('/tmp/' + FILE_NAME, FILE_NAME)

'''
lambda_handler

Driver function for lambda execution
'''
def lambda_handler(event, context):
    main()

main()
