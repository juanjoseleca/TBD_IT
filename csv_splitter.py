import os

def split(filehandler, delimiter=',', user_limit=50, 
    output_name_template='output_%s.csv', output_path='.', keep_headers=True):
    import csv
    reader = csv.reader(filehandler, delimiter=delimiter)
    counter = 1
    count=1
    current_user = ''

    current_out_path = os.path.join(
         output_path,
         output_name_template  % count
    )
    current_out_writer = csv.writer(open(current_out_path, 'w'), delimiter=delimiter)
    
    if keep_headers:
        headers = next(reader)
        current_out_writer.writerow(headers)

    
    flag=True
    for i, row in enumerate(reader):
        if flag==True:
            current_user=row[0].replace(" ", "") 
            flag=False

        if current_user != row[0].replace(" ", "") :
            counter =counter+ 1
            count =count + 1
            flag=True
            if counter % user_limit == 0 :
                current_out_path = os.path.join(
                output_path,
                output_name_template  %count
                )
                current_out_writer = csv.writer(open(current_out_path, 'w'), delimiter=delimiter)
                if keep_headers:
                    current_out_writer.writerow(headers)
        current_out_writer.writerow(row)
split(open('ratings.csv', 'r'))
