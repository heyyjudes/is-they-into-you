# Data Processing helper functions
import os 
import time 
import json 

def transform_to_temporal(temporal_path, root_dir): 
    
    #sort by year, month, day 
    date_dict = {} 
    for filename in os.listdir(root_dir):

        if not filename.startswith('.'): 
            with open(root_dir + filename + '/message.json') as f:
                data = json.load(f)
                if len(data['participants']) == 2: 
                    msg_type = 'Direct'
                    send_receive = {} 
                    send_receive[data['participants'][0]['name']] = data['participants'][1]['name']
                    send_receive[data['participants'][1]['name']] = data['participants'][0]['name']
                else: 
                    msg_type = 'Group'

                for msg in data['messages']:
                    timestamp = int(msg['timestamp_ms']/1000)
                    year = time.gmtime(timestamp).tm_year
                    month = time.gmtime(timestamp).tm_mon
                    day = time.gmtime(timestamp).tm_mday 
                    key = "{:04d}-{:02d}-{:02d}".format(year, month, day)

                    msg['type'] = msg_type
                    if msg_type == 'Direct' and 'sender_name' in msg.keys(): 
                        if msg['sender_name'] not in send_receive.keys(): 
                            # skipping messages sent by participants no longer in conversation
                            pass 
                            #print(filename, msg['sender_name'])
                        else: 
                            msg['receiver_name'] = send_receive[msg['sender_name']]
                    if key in date_dict:
                        date_dict[key].append(msg)
                    else: 
                        date_dict[key] = [msg]


    # save as json files 
    for day in date_dict.keys(): 
        with open(temporal_path + day + '.json', 'w') as f: 
            sorted_list = date_dict[day][:]
            sorted_list.sort(key=lambda x: x['timestamp_ms'])
            json.dump(sorted_list, f)

def count_sent_recieved(data, sender_name, direct_only=False):
    count_send = 0 
    count_recieved = 0 
    for msg in data: 
        if 'sender_name' in msg.keys(): 
            if msg['sender_name'] == sender_name: 
                if direct_only and msg['type'] == 'Direct':
                    count_send += 1 
                elif not direct_only: 
                    count_send += 1 
                else: 
                    pass 
            else: 
                if direct_only and msg['type'] == 'Direct':
                    count_recieved += 1 
                elif not direct_only:
                    count_recieved += 1 
                else: 
                    pass 
    return count_send, count_recieved
