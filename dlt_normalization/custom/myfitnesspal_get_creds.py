import browsercookie
import json
import http.cookiejar as cookiejar

# Extracts all the user cookies from the browser and saves them in a file
def download_cookies():

    cookies = browsercookie.load()
    cookiefiles = []

    # Add the cookies to the CookieJar object
    for cookie in cookies:
        cookie_dict = cookie.__dict__
        cookie_dict['rest'] = cookie_dict['_rest']
        del cookie_dict['_rest']
        cookiefiles.append(cookie_dict)

    # Save the cookies to the JSON file
    with open('dlt_normalization/.env/cookies.json', 'w') as f:
        json.dump(cookiefiles, f)

# Calling the function
download_cookies()