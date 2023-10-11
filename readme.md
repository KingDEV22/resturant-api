## Overview

Based on the data provided, I made some assumtion.

- If all the provided timestamp for a store in the dataset is active then it is active all the time and vice versa
- If all the provided timestamp for a store for a is active then it is active for the day and vice versa.

#### Endpoints

`/api/file/hours/add` - to add the store hours from a csv file.

_Request Body_ - Multipart/form-data

```
file * string($binary)
```

_Response Body_

Status Code - 200 Success , 422 - ERROR

`/api/file/status/add` - to add the store status from a csv file.

_Request Body_ - Multipart/form-data

```
file * string($binary)
```

_Response Body_

Status Code - 200 Success , 422 - ERROR

`/api/file/time/add` - to add the stores timezone from a csv file.

_Request Body_ - Multipart/form-data

```
file * string($binary)
```

_Response Body_

Status Code - 200 Success , 422 - ERROR

`/api/report/trigger_report` - to trigger the report generation for the data

_Response Body_

```
{
    "report_id": "1605984098"
}
```
Status Code - 200 Success 

`/api/report/get_report endpoint/{report_id}` - gives the output as running if report generation is in progress else return the report in csv file.

Status Code - 200 Success, 404 error
