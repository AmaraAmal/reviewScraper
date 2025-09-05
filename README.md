# ReviewScraper

## Project Overview
Welcome to the ReviewScraper project! 
This Python based tool leverages the power of Playwright to scrape and extract valuable data from various websites such as Google Maps, Amazon, Facebook, Yelp and many more..

## Getting Started
To get started with ReviewScraper follow the installation provided in the installation section below. Once you have the tool set up, head over to the usage section followed by the code structure section to learn more about the code provided.

## Prerequisites
- Python (version 3.11.2) installed
- pip (version 23.1.2) installed

## Installation

`pip install -r requirements.txt`
 
`playwrigth install`

## Usage
To run the app type in the following command into the terminal :

`python Flask.py`

## Code Structure and Functionality
In this section we will go through the different classes and functions used in the project.

**GetReviews**

The 'GetReviews' class serves as the main class encapsulating the different functions used for scraping reviews.
It offers the following methods and functions : 

<u>**Initialisation**</u>
- **init**(self)
: Initialize the class instance with the following variables :

  1. *self.hrs* : The number of hours associated to the expiration period
  2. *self.useragent_list* : A static list of different user agents
  3. *self.useragent* : Takes a random useragent from the useragent_list and affects it to the class functions if called
  4. *self.link* : A link used for testing the proxies before passing them to the scraping functions

<u>**Proxy Test**</u>
- **get_valid_proxy**(self, proxies, link)
: Goes through each IP address and check its functionality returning the valid proxy. In case no operational one was found it returns None.

- **test_proxy**(self, proxy, link)
: A boolean function for testing the proxies , it sends a "Get request" to the link defined in the **init** methode using the "httpx" package and checks its status. Returns True when receiving a (200 OK) status code and False otherwise.

<u>**Scraping Main Functions**</u>
- **get_google_reviews**(self, place_id, data_dict, job_id)
- **get_airbnb_reviews**(self, link, data_dict, job_id)
- **get_ebay_reviews**(self, link, data_dict, job_id)
- **get_etsy_reviews**(self, link, data_dict, job_id)
- **get_play_store_reviews**(self, link, data_dict, job_id)
- **get_trip_advisor_reviews**(self, link, data_dict, job_id)
- **get_capterra_reviews**(self, link, data_dict, job_id)
- **get_g2_reviews**(self, link, data_dict, job_id)
- **get_amazon_reviews**(self, link, data_dict, job_id)
- **get_facebook_reviews**(self, link, data_dict, job_id)
- **get_trustpilot_reviews**(self, link, data_dict, job_id)
- **get_yelp_reviews**(self, link, data_dict, job_id)
- **get_booking_reviews**(self, link, data_dict, job_id)

<u>**Scraping Sub Functions**</u>

These functions are used when a website has different page structures.
- **airbnb_sub_function**(self, page, data_dict, job_id)
- **scrape_trip_advisor_attraction_reviews**(self, page, data_dict, job_id)
- **scrape_trip_advisor_airline_reviews**(self, page, data_dict, job_id)
- **scrape_trip_advisor_hotel_reviews**(self, page, data_dict, job_id)
- **scrape_trip_advisor_restaurant_reviews**(self, page, data_dict, job_id)
- **scrape_trip_advisor_rental_reviews**(self, page, data_dict, job_id)
- **scrape_capterra_service_reviews**(self, page, data_dict, job_id)
- **scrape_capterra_other_reviews**(self, page, data_dict, job_id)

## Code Walkthrough
In this section, we will walk through some key code snippets from the ReviewScraper project, explaining their functionality and usage.

**Creating a Proxy Parameter**

To create a proxy parameter and pass it to the headless browser follow this code:  
``` python 
proxy = ProxySettings(server=valid_proxy, username=self.username, password=self.password)
```

**Managing cookies**

To maintain a user sessions across different sessions we opt to save the state of cookies which can be done using the following code line:
``` python 
await context.storage_state(path="cookies.json")
```
Once the cookies are saved into the json file they should get applied to the context as mentioned in the context creation section.

**Launching a Headless Browser**

To initiate a headless browser using Playwright, you can use the following line of code:
``` python 
browser = await p.chromium.launch(headless=True)
```
To pass a proxy to the browser:
``` python 
browser = await p.chromium.launch(headless=True, proxy=proxy)
```

**Creating a Browser Context**

To create a browser context:

- *locale='en-GB*' : Specify user locale to set the navigator language
- *user_agent* : Pass a user agent to the context, in our case we are passing the **self.useragent** created in the __init__ methode
- *bypass_csp* : Bypassing page's Content-Security-Policy. Defaults to False 
- *storage_state* : Setting the page necessary cookies
``` python 
context = await browser.new_context(locale='en-GB', user_agent=useragent, bypass_csp=True, storage_state='site_cookies.json')
```

**Creating a New Page**

To create a new page use the following code line:
``` python 
page = await context.new_page()
```

**Navigating to a Web Page**

Once you have the browser instance, you can navigate to a specific web page using the following code:
``` python 
await page.goto(link, timeout=0)
```

**Extracting Review Elements**

To extract review elements from the page, you can use Playwright's DOM manipulation function:
``` python 
review = await page.query_selector("selector")
```

For extracting multiple elements :
``` python 
reviews = await page.query_selector_all("selector")
```

**Extracting Textual Content from Elements**

To extract the visible text content within an HTML tags use this code:
``` python 
text = await element.inner_text()
```

**Extracting Attributes from Elements**

To extract one of the attributes within an HTML tags follow this code line:
``` python 
attribute = await element.get_attribute("attribute_name")
```

**Capturing Navigator Screenshots**

To provide a visual record of the state of the page during the scraping process we can capture a screenshot using the following line:
``` python 
await page.screenshot(path="screenshot.png", full_page=True)
```

## Troubleshooting
If you encounter any issues while using the ReviewScraper project, here are some common problems and solutions that may help:

**1. Installation Problems**

**Issue:** Facing difficulties related to the project libraries.

**Solution:** Make sure the required dependencies are satisfied. Consider updating the libraries if necessary

**2. Headless Browser Not Launching**

**Issue:** The headless browser fails to launch when using Playwright.

**Solution:** Ensure that the browser is getting a functional proxy and that the headless parameter is set to True

**3. General Stats Error**

**Issue:** Fails to get the site general stats.

**Solution:** Make sure that the Selectors are contemporary

**4. Performing Scroll Error**

**Issue:** Fails to perform the necessary scroll for scraping reviews.

**Solution:** Make sure that the Selectors are contemporary

**5. Scraping Error**

**Issue:** Scraping reviews fails.

**Solution:** Make sure that the Selectors are contemporary

**6. Loading Reviews Error**

**Issue:** Loading the reviews page fails.

**Solution:** Make sure that the Selectors related to the loading reviews part are contemporary

**7. Handling Translations / Setting Language Error**

**Issue:** Handling Translations in AirBnb or Trip Advisor sites fails.

**Solution:** Make sure that the Selectors related to the translation code part are contemporary

**8. Incorrect Data Extraction**

**Issue:** The extracted review data doesn't match the expected content.

**Solution:** Double-check the Selectors and the element queries

**9.  Empty or Missing Data**

**Issue:** The code runs successfully, but some data are missing or empty.

**Solution:** Review the page's HTML structure and make sure it has not changed

**10. Site Inaccessible**

**Issue:** Accessing the site is blocked by a Captcha.

**Solution:** Update the cookies file by following the steps mentioned in **Managing Cookies** section

**11. Other Type of Errors**

**Issue:** You encounter an issue that's not covered here.

**Solution:** Run the code on local with interface (set the headless browser parameter to False) and keep track of what is going on during the scraping. If the issue persist please feel free to get in touch with the code developer

## Related Resources
Explore these additional resources to enhance your understanding of the project.

[Playwright Introduction](https://playwright.dev/python/docs/intro)

[Playwright API Reference](https://playwright.dev/python/docs/api/class-playwright)