import boto3
import csv
import io
from boto3.dynamodb.conditions import Key, Attr
from collections import defaultdict
from datetime import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    try:
        start_date = event['start_date']
        end_date = event['end_date']
        result = query_signin_count(start_date, end_date)
        return {
            'statusCode': 200,
            'body': result
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': str(e)
        }
        
def query_signin_count(start_date,end_date):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('xilin-teacher-signin-table')
    assignment_table = dynamodb.Table('xilin-teach-class-assignment')
    teacher_table = dynamodb.Table('xilin-teacher')
    class_table = dynamodb.Table('xilin-classes')

    # Initialize variables for pagination and results
    signin_records = defaultdict(dict)
    signin_counts = defaultdict(int)
    all_signin_dates = defaultdict(bool) # Dict to keep track of all dates 
    all_teacher_class_assignments = defaultdict(list) # Dict to keep track of all teacher-class assignments

    last_evaluated_key = None

    while True:
        # Prepare scan parameters
        scan_params = {
            'FilterExpression': Attr('signin_date').between(start_date,end_date),
            'ProjectionExpression': 'teacher_id, signin_date, signin_time'
        }

        # Add ExclusiveStartKey for pagination if it exists
        if last_evaluated_key:
            scan_params['ExclusiveStartKey'] = last_evaluated_key

        # Perform the scan
        response = table.scan(**scan_params)

        
        # Process items
        for item in response['Items']:
            teacher_id = item['teacher_id']
            signin_date = item.get('signin_date', '')
            
            # Keep track of all the dates we visited
            all_signin_dates[signin_date] = True
            
            signin_time = item.get('signin_time', '')
            signin_counts[teacher_id] += 1
            
            # Get teacher info
            try:
                teacher_response = teacher_table.get_item(
                    Key={'teacher_id': teacher_id}
                )
                teacher_info = teacher_response.get('Item', {})
                first_name = teacher_info.get('first_name', '')
                last_name = teacher_info.get('last_name', '')
                teacher_type = teacher_info.get('type', '')
            except Exception as e:
                first_name = ''
                last_name = ''
                teacher_type = ''
            
            # Query assignment table for all payRates for this teacher
            try:
                assignment_response = assignment_table.query(
                    KeyConditionExpression=Key('teacherId').eq(teacher_id)
                )
                
                total_minutes = 0
                class_info_list = []
                
                for assignment in assignment_response['Items']:
                    
                    
                    classId = assignment.get('classId', '')
                    
                    all_teacher_class_assignments[(teacher_id, classId)] = [last_name + ', ' + first_name, teacher_type]
                    
                    # Query class table for class info
                    try:
                        class_response = class_table.get_item(
                            Key={'classId': classId}
                        )
                        class_info = class_response.get('Item', {})
                        class_name = class_info.get('description', '')
                        duration = float(class_info.get('duration', 0))
                        start_time = class_info.get('startTime', '')
                        end_time = class_info.get('endTime', '')
                        
                        all_teacher_class_assignments[(teacher_id, classId)] += [class_name, start_time, end_time, duration]
                        
                        # Check if signin_time is before class startTime
                        if signin_time and start_time:
                            # try:
                            #     # Parse signin_time (try multiple formats)
                            #     signin_dt = None
                            #     for fmt in ['%I:%M:%S %p', '%I:%M %p', '%H:%M:%S', '%H:%M']:
                            #         try:
                            #             signin_dt = datetime.strptime(signin_time, fmt)
                            #             break
                            #         except ValueError:
                            #             continue
                                
                            #     # Parse start_time (try multiple formats)
                            #     start_dt = None
                            #     for fmt in ['%I:%M:%S %p', '%I:%M %p', '%H:%M:%S', '%H:%M']:
                            #         try:
                            #             start_dt = datetime.strptime(start_time, fmt)
                            #             break
                            #         except ValueError:
                            #             continue
                                
                            #     if signin_dt and start_dt and signin_dt < start_dt:
                            total_minutes += duration
                            signin_record = {
                                'teacher_id': teacher_id,
                                'name': last_name + ', ' + first_name,
                                'teacher_type': teacher_type,
                                'signin_date': signin_date,
                                'signin_time': signin_time,
                                'classId': classId,
                                'class_name': class_name,
                                'duration': duration,
                                'start_time': start_time,
                                'end_time': end_time,
                            }
                            signin_records[(teacher_id, classId, signin_date)] = signin_record
                                    
                            # except ValueError:
                            #     pass  # Skip invalid time formats
                            
                    except Exception as e:
                        print(f"Error getting class info for {classId}: {str(e)}")
            
            except Exception as e:
                print(f"Error processing teacher {teacher_id}: {str(e)}")

        # Check if there are more items to retrieve
        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break
        
        
    # Save to S3 as CSV
    csv_filename = f"signin-report-{start_date}-{end_date}.csv"
    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)
    
    
    top_header = ['', '']
    headers = ['Teacher ID', 'Name']
    for date in sorted(all_signin_dates.keys()):
        headers.append(date)
        # headers.append(date + ' Sign-in')
        top_header += ['Minutes']
    headers += ['Teacher Type']
    csv_writer.writerow(top_header)
    csv_writer.writerow(headers)
    
    
    for (teacher_id, classId) in sorted(all_teacher_class_assignments.keys()):
        assignment_values = all_teacher_class_assignments[(teacher_id, classId)]
        
        row = [teacher_id, assignment_values[0]]
        
        for possible_date in sorted(all_signin_dates.keys()):
            this_class_on_this_date = signin_records.get((teacher_id, classId, possible_date), None)
            
            if this_class_on_this_date is None:
                row += ['0']
            else:
                row += [this_class_on_this_date['duration']]
        
        row += [assignment_values[1]]
        
        csv_writer.writerow(row)
    
    logger.info(f"CSV generation completed. Starting S3 upload process.")
    
    # Upload to S3
    logger.info(f"Attempting to upload CSV to S3: {csv_filename}")
    s3 = boto3.client('s3')
    csv_content = csv_buffer.getvalue()
    s3.put_object(
        Bucket='xilinnw-payment',
        Key=csv_filename,
        Body=csv_content,
        ContentType='text/csv'
    )

    logger.info("Successfully uploaded to S3!")
    # Generate signed URL for download
    download_url = s3.generate_presigned_url(
        'get_object',
        Params={'Bucket': 'xilinnw-payment', 'Key': csv_filename},
        ExpiresIn=20
    )
    
    return {
        # 'payment_data': final_teacher_pay,
        'download_url': download_url
    }
