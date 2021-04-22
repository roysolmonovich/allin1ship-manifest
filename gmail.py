from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
import base64
import json
from os import path, remove
from zipfile import ZipFile
from lib import update_amp, del_con_to_dims, tracking_to_acc, overlabeled_report_update
with open(r'invoices\marked_up.json', 'rb') as f:
    marked_up_log = json.load(f)
with open(r'invoices\overlabeled.json', 'rb') as f:
    overlabeled_log = json.load(f)
with open(r'invoices\shipping_variance.json', 'rb') as f:
    shipping_variance_log = json.load(f)
customer_tracking_log = {}
SCOPES = 'https://www.googleapis.com/auth/gmail.readonly'


def process_attachments(service, message, label):
    parts = [message['payload']]
    while parts:
        part = parts.pop()
        if part.get('parts'):
            parts.extend(part['parts'])
        if part.get('filename'):
            if 'data' in part['body']:
                file_data = base64.urlsafe_b64decode(part['body']['data'].encode('UTF-8'))
            elif 'attachmentId' in part['body']:
                attachment = service.users().messages().attachments().get(
                    userId='me', messageId=message['id'], id=part['body']['attachmentId']
                ).execute()
                file_data = base64.urlsafe_b64decode(attachment['data'].encode('UTF-8'))
            else:
                file_data = None
            if file_data:
                if label == 'Unknown':
                    if 'Shipment_Variance_Report' in part["filename"]:
                        label = 'Shipping Variance'
                    elif 'Marked_Up_Items' in part["filename"]:
                        label = 'Marked Up Items'
                    elif 'Over_label_Items' in part["filename"]:
                        label = 'Overlabeled'
                filepath = fr'dependencies\{label}\{part["filename"]}'
                if label == 'Marked Up Items' and not(marked_up_log.get(part['filename'][:8]) and
                                                      part['filename'] in marked_up_log[part['filename'][:8]]):
                    print('Importing %s. File size: %s' %
                          (part['filename'], attachment['size']))
                    with open(filepath, 'wb') as f:
                        f.write(file_data)
                    with ZipFile(filepath) as zf:
                        zf.extract(part['filename'][:-4], rf'dependencies\{label}')
                    del_con_to_dims(filepath.rstrip('.zip'))
                    if not marked_up_log.get(part['filename'][:8]):
                        marked_up_log[part['filename'][:8]] = [part['filename']]
                    else:
                        marked_up_log[part['filename'][:8]].append(part['filename'])
                    with open(r'invoices\marked_up.json', 'w') as fw:
                        json.dump(marked_up_log, fw, indent=4)
                    print('Imported successfully.')
                    remove(filepath)
                elif 'Shipping Variance' in label and not(shipping_variance_log.get(part['filename'][:8]) and
                                                          part['filename'] in shipping_variance_log[part['filename'][:8]]):
                    print('Importing %s. File size: %s' %
                          (part['filename'], attachment['size']))
                    with open(filepath, 'wb') as f:
                        f.write(file_data)
                    with ZipFile(filepath) as zf:
                        zf.extract(part['filename'][:-4], rf'dependencies\{label}')
                    update_amp(filepath.rstrip('.zip'))
                    if not shipping_variance_log.get(part['filename'][:8]):
                        shipping_variance_log[part['filename'][:8]] = [part['filename']]
                    else:
                        shipping_variance_log[part['filename'][:8]].append(part['filename'])
                    if 'Weekly' in label:
                        shipping_variance_log[part['filename'][:8]].append('Weekly')
                        print('Imported weekly file successfully.')
                    else:
                        print('Imported successfully.')
                    with open(r'invoices\shipping_variance.json', 'w') as fw:
                        json.dump(shipping_variance_log, fw, indent=4)
                elif label == 'Customer Tracking':
                    with open(filepath, 'wb') as f:
                        f.write(file_data)
                    tracking_to_acc(filepath)
                    remove(filepath)
                elif label == 'Overlabeled' and not(overlabeled_log.get(part['filename'][:8]) and
                                                    part['filename'] in overlabeled_log[part['filename'][:8]]):
                    print('Importing %s. File size: %s' %
                          (part['filename'], attachment['size']))
                    with open(filepath, 'wb') as f:
                        f.write(file_data)
                    with ZipFile(filepath) as zf:
                        zf.extract(part['filename'][:-4], rf'dependencies\{label}')
                    overlabeled_report_update(filepath)
                    if not overlabeled_log.get(part['filename'][:8]):
                        overlabeled_log[part['filename'][:8]] = [part['filename']]
                    else:
                        overlabeled_log[part['filename'][:8]].append(part['filename'])
                    with open(r'invoices\overlabeled.json', 'w') as fw:
                        json.dump(overlabeled_log, fw, indent=4)
                    print('Imported successfully.')
                    remove(filepath)


def process_emails():

    store = file.Storage('token.json')
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets('gmail_credentials.json', SCOPES)
        creds = tools.run_flow(flow, store)
    service = build('gmail', 'v1', http=creds.authorize(Http()))
    # Call the Gmail API to fetch INBOX
    results = service.users().messages().list(userId='me', labelIds=['INBOX']).execute()
    messages = results.get('messages', [])
    for message in messages:
        msg = service.users().messages().get(userId='me', id=message['id']).execute()
        if 'Label_4117247183215098953' in msg['labelIds']:  # 'Shipping Variance' label in Gmail
            process_attachments(service, msg, 'Shipping Variance')
        elif 'Label_899220777112089110' in msg['labelIds']:  # 'Marked Up Items' label in Gmail
            process_attachments(service, msg, 'Marked Up Items')
        elif 'Label_1481354917256413378' in msg['labelIds']:
            process_attachments(service, msg, 'Customer Tracking')
        elif 'Label_3956799254482150245' in msg['labelIds']:
            process_attachments(service, msg, 'Overlabeled')
        elif 'Label_4225615189422408527' in msg['labelIds']:
            process_attachments(service, msg, 'Shipping Variance Weekly')
        else:
            process_attachments(service, msg, 'Unknown')
        # else:
        #     if 'parts' in msg['payload']:
        #         for p in msg['payload']['parts']:
        #             if 'body' in p and 'attachmentId' in p['body']:
        #                 print(p['filename'])
        # else:
            # if 'IMPORTANT' in msg['labelIds']:
            #     if 'parts' in msg['payload']:
            #         for p in msg['payload']['parts']:
            #             if 'body' in p and 'attachmentId' in p['body']:
            #                 print(p['filename'])


# process_emails()
