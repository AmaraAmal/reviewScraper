import asyncio
import random
import re
import datetime
import time
import timedelta
from playwright.async_api import async_playwright
import Selectors
import logging

logging.basicConfig(filename='app.log', level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("app")


class GetReviews:
    def __init__(self, ):
        self.hrs = 72
        self.entities = {}
        self.status_dict = {}
        self.resp_type = ""
        self.status = ""
        self.no_reviews = True
        self.useragent_list = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 "
            "Safari/537.36 Edge/12.246",
            "Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 (KHTML, like Gecko) Version/9.0.2 Safari/601.3.9",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0.1"
        ]
        self.useragent = random.choice(self.useragent_list)

    async def get_google_reviews(self, place_id, data_dict, job_id):
        logger.info("STARTED SCRAPING..")
        async with async_playwright() as p:
            try:
                browser = await p.chromium.launch(headless=False)
                logger.info("BROWSER LAUNCHED..")
                context = await browser.new_context(locale='en-GB', user_agent=self.useragent, bypass_csp=True)

                provider = 'Google'
                created_at = datetime.datetime.now()
                expiration = created_at + timedelta.Timedelta(hours=self.hrs)
                expiration = expiration.isoformat()
                updated_at = created_at.isoformat()
                created_at = created_at.isoformat()
                new_uuid = job_id

                reviews = []
                crushed_scroll = False

                page = await context.new_page()
                logger.info("PAGE CREATED..")
                google_link = "https://www.google.com/maps/search/?api=1&query=Google&query_place_id=" + place_id
                page.set_default_navigation_timeout(timeout=0)
                await page.goto(google_link, timeout=0)
                logger.info("NAVIGATED TO LINK.. %s", google_link)
                await asyncio.sleep(3)
            except Exception as exception:
                logger.info(exception)
                self.status = "Browser Couldn't Start"
                self.status_dict['status'] = self.status
                data_dict[job_id] = self.status_dict
                return self.status
            else:
                entities = {'keyword': place_id, 'provider': provider, 'link': page.url, 'uuid': new_uuid,
                            'expiration': str(expiration), 'created_at': str(created_at), 'updated_at': str(updated_at)}
                data_dict[job_id] = entities
                page_link = page.url
                hotel_index = page_link.find("Hotel")

                logger.info("CHECKING AVAILABILITY OF REVIEWS..")
                btns = await page.query_selector_all(Selectors.google_btns)
                for i in range(0, len(btns)):
                    if await btns[i].inner_text() == "Reviews":
                        await btns[i].click()
                        await asyncio.sleep(3)
                        self.no_reviews = False
                if self.no_reviews is True:
                    self.status = "No Reviews Found"
                    self.status_dict['status'] = self.status
                    data_dict[job_id] = self.status_dict
                    return self.status
                else:
                    try:
                        logger.info("GETTING GENERAL STATS..")
                        stats_div = await page.query_selector(Selectors.google_stats_div)
                        gen_rating_div = await stats_div.query_selector(Selectors.google_gen_rating_div)
                        gen_rating = await gen_rating_div.inner_text()
                        tot_rev_div = await stats_div.query_selector(Selectors.google_tot_rev_div)
                        tot_rev_txt = await tot_rev_div.inner_text()
                        total_rev = re.sub('[^0-9]', '', tot_rev_txt)

                        entities['general rating'] = float(gen_rating)
                        entities['total reviews'] = int(total_rev)
                    except Exception as exception:
                        logger.info(exception)
                        self.status = "An Error Occurred While Getting General Stats"
                        self.status_dict['status'] = self.status
                        data_dict[job_id] = self.status_dict
                        return self.status
                    else:
                        try:
                            logger.info("PERFORMING SCROLL..")
                            start_time = time.time()
                            rev_len = 0
                            while True:
                                rev_div = await page.query_selector_all(Selectors.google_rev_div)
                                await page.mouse.wheel(0, 15000)
                                await asyncio.sleep(3)
                                if rev_len == len(rev_div):
                                    crushed_scroll = True
                                rev_len = len(rev_div)
                                current_time = time.time()
                                timeout = current_time - start_time >= 60
                                if len(rev_div) >= 100 or len(rev_div) >= int(
                                        total_rev) or timeout or crushed_scroll:
                                    break
                        except Exception as exception:
                            logger.info(exception)
                            self.status = "Error Performing Scroll"
                            self.status_dict['status'] = self.status
                            data_dict[job_id] = self.status_dict
                            return self.status
                        else:
                            try:
                                logger.info("SCRAPING REVIEWS..")
                                for i in range(len(rev_div)):
                                    img_div = await rev_div[i].query_selector(Selectors.google_img_div)
                                    names_div = await rev_div[i].query_selector(Selectors.google_names_div)
                                    if hotel_index != -1:
                                        rate_div = await rev_div[i].query_selector(Selectors.google_hotel_rate_span)
                                        date_div = await rev_div[i].query_selector(Selectors.google_hotel_date_span)
                                    else:
                                        rate_div = await rev_div[i].query_selector(Selectors.google_rate_div)
                                        date_div = await rev_div[i].query_selector(Selectors.google_date_div)
                                    expr_div = await rev_div[i].query_selector(Selectors.google_expr_div)
                                    rev_id_div = await rev_div[i].query_selector(Selectors.google_rev_id_div)
                                    more_btn = await rev_div[i].query_selector(Selectors.google_more_btn)
                                    await more_btn.click()
                                    translate_btn = await rev_div[i].query_selector_all(
                                        Selectors.google_translate_btn)
                                    await translate_btn[0].click()
                                    review = {'image': await img_div.get_attribute("src"),
                                              'name': await names_div.inner_text()}
                                    if hotel_index != -1:
                                        rate_txt = await rate_div.inner_text()
                                    else:
                                        rate_txt = await rate_div.get_attribute("aria-label")
                                    review['rate'] = float(rate_txt[0])
                                    review['date'] = await date_div.inner_text()
                                    try:
                                        review['experience'] = await expr_div.inner_text()
                                    except:
                                        review['experience'] = None

                                    rev_id_href = await rev_id_div.get_attribute('data-href')
                                    start_id_index = rev_id_href.find('contrib')
                                    end_id_index = rev_id_href.find('reviews')
                                    rev_id_txt = rev_id_href[start_id_index + 8:end_id_index - 1]

                                    review['review_id'] = rev_id_txt
                                    review["provider"] = provider
                                    reviews.append(review)
                                    entities['reviews'] = reviews
                                    data_dict[job_id] = entities
                            except Exception as exception:
                                logger.info(exception)
                                self.status = "An Error Occurred During Getting Reviews"
                                self.status_dict['status'] = self.status
                                data_dict[job_id] = self.status_dict
                                return self.status
                            else:
                                logger.info("SCRAPING DONE")
                                logger.info("REVIEWS SCRAPED = %s", len(reviews))
                                entities['reviews'] = reviews
            finally:
                await context.close()
                logger.info("CONTEXT CLOSED")
                await browser.close()
                logger.info("BROWSER CLOSED")
            data_dict[job_id] = entities
            return data_dict

    async def airbnb_sub_function(self, page, data_dict, job_id):
        scraped_reviews = []
        default_rev_div = await page.query_selector(Selectors.airbnb_default_rev_div)
        try:
            logger.info("GETTING GENERAL STATS..")
            stats_div = await default_rev_div.query_selector(Selectors.airbnb_sub_funct_stats_div)
            stats_txt = await stats_div.inner_text()
            gen_rate_test = await page.query_selector(Selectors.airbnb_sub_funct_gen_rate_test)
            if gen_rate_test is not None:
                gen_rating = 0.0
                total_rev = stats_txt[0]
            else:
                gen_rating = stats_txt[:3]
                rev_index = stats_txt.find("from")
                tot_rev_txt = stats_txt[rev_index + 5:]
                total_rev = re.sub('[^0-9]', '', tot_rev_txt)
        except Exception as exception:
            logger.info(exception)
            self.resp_type = "status"
            self.status = "Error Getting General Stats"
            self.status_dict['status'] = self.status
            data_dict[job_id] = self.status_dict
            return self.status
        else:
            try:
                logger.info("SCRAPING REVIEWS..")
                revs_div = await page.query_selector_all(Selectors.airbnb_sub_funct_revs_div)
                for j in range(len(revs_div)):
                    imgs_div = await revs_div[j].query_selector_all(Selectors.airbnb_imgs_div)
                    names_div = await revs_div[j].query_selector_all(Selectors.airbnb_names_div)
                    dates_div = await revs_div[j].query_selector_all(Selectors.airbnb_dates_div)
                    exprs_div = await revs_div[j].query_selector_all(Selectors.airbnb_exprs_div)
                    review = {}
                    try:
                        review['image'] = await imgs_div[0].get_attribute("src")
                    except:
                        review['image'] = None
                    review['name'] = await names_div[0].inner_text()
                    review['date'] = await dates_div[0].inner_text()
                    review['experience'] = await exprs_div[0].inner_text()
                    review["provider"] = "AirBnB"
                    scraped_reviews.append(review)
                    data_dict[job_id]['reviews'] = scraped_reviews
            except Exception as exception:
                logger.info(exception)
                self.resp_type = "status"
                self.status = "An Error Occurred During Getting Reviews"
                self.status_dict['status'] = self.status
                data_dict[job_id] = self.status_dict
                return self.status
        return float(gen_rating), int(total_rev), scraped_reviews

        # AIRBNB: DONE -- IN TESTING PHASE

    async def get_airbnb_reviews(self, link, data_dict, job_id):
        async with async_playwright() as p:
            try:
                browser = await p.chromium.launch(headless=True)
                logger.info("BROWSER LAUNCHED..")
                context = await browser.new_context(locale='en-GB', user_agent=self.useragent, bypass_csp=True)

                provider = 'AirBnB'
                created_at = datetime.datetime.now()
                expiration = created_at + timedelta.Timedelta(hours=self.hrs)
                expiration = expiration.isoformat()
                updated_at = created_at.isoformat()
                created_at = created_at.isoformat()
                new_uuid = job_id
                reviews = []
                crushed_scroll = False

                page = await context.new_page()
                logger.info("PAGE CREATED..")
                page.set_default_navigation_timeout(timeout=0)
                await page.goto(link, timeout=0)
                logger.info("NAVIGATED TO LINK.. %s", link)
                await asyncio.sleep(10)
            except Exception as exception:
                logger.info(exception)
                self.status = "Browser Couldn't Start"
                self.status_dict['status'] = self.status
                data_dict[job_id] = self.status_dict
                return self.status
            else:
                entities = {'provider': provider, 'keyword': link, 'link': page.url, 'uuid': new_uuid,
                            'expiration': str(expiration), 'created_at': str(created_at), 'updated_at': str(updated_at)}
                data_dict[job_id] = entities
                logger.info("HANDLING TRANSLATIONS..")
                try:
                    translate = await page.query_selector(Selectors.airbnb_translate)
                    if translate is not None:
                        settings = await page.query_selector(Selectors.airbnb_settings)
                        await settings.click()
                        await asyncio.sleep(2)
                        auto_trans = await page.query_selector(Selectors.airbnb_auto_trans)
                        await auto_trans.click()
                        await asyncio.sleep(3)
                    logger.info("TRANSLATION HANDLED.")
                except Exception as exception:
                    logger.info(exception)
                    self.status = "Error Handling Translations"
                    self.status_dict['status'] = self.status
                    data_dict[job_id] = self.status_dict
                    return self.status
                else:
                    await page.keyboard.press("End")
                    await asyncio.sleep(3)
                    logger.info("CHECKING AVAILABILITY OF REVIEWS..")
                    check = await page.query_selector(Selectors.airbnb_check)
                    default_rev_div = await page.query_selector(Selectors.airbnb_default_rev_div)
                    if check is not None or default_rev_div is None:
                        self.status = "No Reviews Found"
                        self.status_dict['status'] = self.status
                        data_dict[job_id] = self.status_dict
                        return self.status
                    try:
                        logger.info("LOADING REVIEWS..")
                        await asyncio.sleep(1)
                        show_more = await default_rev_div.query_selector(Selectors.airbnb_show_more)
                        if show_more is None:
                            logger.info("REDIRECTING TO SUB FUNCTION..")
                            if self.resp_type == "status":
                                return self.status
                            else:
                                entities['general rating'], entities['total reviews'], entities[
                                    'reviews'] = await self.airbnb_sub_function(page, data_dict, job_id)
                                data_dict[job_id] = entities
                                logger.info("SCRAPING DONE")
                                return data_dict
                        else:
                            await show_more.click()
                            await asyncio.sleep(1)
                    except Exception as exception:
                        logger.info(exception)
                        self.status = "Error Loading Reviews"
                        self.status_dict['status'] = self.status
                        data_dict[job_id] = self.status_dict
                        return self.status
                    else:
                        logger.info("GETTING GENERAL STATS..")
                        try:
                            popup = await page.query_selector(Selectors.airbnb_popup)
                            gen_div = await popup.query_selector(Selectors.airbnb_gen_div)
                            full_gen_txt = await gen_div.inner_text()

                            first_number_match = re.search(r'(\d+\.\d+)', full_gen_txt)
                            second_number_match = re.search(r'(\d+) reviews', full_gen_txt)
                            gen_rating = float(first_number_match.group()) if first_number_match else 5.0
                            total_rev = int(second_number_match.group(1)) if second_number_match else 0

                            entities['general rating'] = gen_rating
                            entities['total reviews'] = total_rev
                        except Exception as exception:
                            logger.info(exception)
                            self.status = "Error Getting General Stats"
                            self.status_dict['status'] = self.status
                            data_dict[job_id] = self.status_dict
                            return self.status
                        else:
                            logger.info("PERFORMING SCROLL..")
                            try:
                                start_time = time.time()
                                rev_len = 0
                                while True:
                                    rev_div = await page.query_selector_all(Selectors.airbnb_rev_div)
                                    await page.mouse.wheel(0, 5000)
                                    await asyncio.sleep(3)
                                    if rev_len == len(rev_div):
                                        crushed_scroll = True
                                    rev_len = len(rev_div)
                                    current_time = time.time()
                                    timeout = current_time - start_time >= 60
                                    if len(rev_div) >= 100 or len(
                                            rev_div) >= total_rev or timeout or crushed_scroll:
                                        break
                            except Exception as exception:
                                logger.info(exception)
                                self.status = "Error Performing Scroll"
                                self.status_dict['status'] = self.status
                                data_dict[job_id] = self.status_dict
                                return self.status
                            else:
                                try:
                                    logger.info("SCRAPING REVIEWS..")
                                    for i in range(len(rev_div)):
                                        imgs_div = await rev_div[i].query_selector_all(Selectors.airbnb_imgs_div)
                                        names_div = await rev_div[i].query_selector_all(Selectors.airbnb_names_div)
                                        dates_div = await rev_div[i].query_selector_all(Selectors.airbnb_dates_div)
                                        exprs_div = await rev_div[i].query_selector_all(Selectors.airbnb_exprs_div)
                                        review = {}
                                        try:
                                            review['image'] = await imgs_div[0].get_attribute("src")
                                        except:
                                            review['image'] = None
                                        review['name'] = await names_div[0].inner_text()
                                        review['date'] = await dates_div[0].inner_text()
                                        review['experience'] = await exprs_div[0].inner_text()
                                        review["provider"] = provider
                                        reviews.append(review)
                                except Exception as exception:
                                    logger.info(exception)
                                    self.status = "An Error Occurred During Getting Reviews"
                                    self.status_dict['status'] = self.status
                                    data_dict[job_id] = self.status_dict
                                    return self.status
                                else:
                                    logger.info("SCRAPING DONE")
                                    logger.info("REVIEWS SCRAPED = %s", len(reviews))
                                    entities['reviews'] = reviews
            finally:
                await context.close()
                logger.info("CONTEXT CLOSED")
                await browser.close()
                logger.info("BROWSER CLOSED")
            data_dict[job_id] = entities
            return data_dict

    async def get_ebay_reviews(self, link, data_dict, job_id):
        async with async_playwright() as p:
            try:
                browser = await p.chromium.launch(headless=True)
                logger.info("BROWSER LAUNCHED..")
                context = await browser.new_context(locale='en-GB', bypass_csp=True, user_agent=self.useragent)

                provider = 'eBay'
                created_at = datetime.datetime.now()
                expiration = created_at + timedelta.Timedelta(hours=self.hrs)
                expiration = expiration.isoformat()
                updated_at = created_at.isoformat()
                created_at = created_at.isoformat()
                new_uuid = job_id
                reviews = []

                page = await context.new_page()
                logger.info("PAGE CREATED..")

                page.set_default_navigation_timeout(timeout=0)
                await page.goto(link, timeout=0)
                logger.info("NAVIGATED TO LINK.. %s", link)
                await asyncio.sleep(10)
            except Exception as exception:
                logger.info(exception)
                self.status = "Browser Couldn't Start"
                self.status_dict['status'] = self.status
                data_dict[job_id] = self.status_dict
                return self.status
            else:
                entities = {'provider': provider, 'keyword': link, 'link': page.url, 'uuid': new_uuid,
                            'expiration': str(expiration), 'created_at': str(created_at), 'updated_at': str(updated_at)}
                data_dict[job_id] = entities

                logger.info("CHECKING AVAILABILITY OF REVIEWS..")
                check = await page.query_selector(Selectors.ebay_check)
                if check is None:
                    self.status = "No Reviews Found"
                    self.status_dict['status'] = self.status
                    data_dict[job_id] = self.status_dict
                    return self.status
                try:
                    await page.keyboard.press('End')
                    logger.info("GETTING GENERAL STATS..")
                    tot_rev_span = await page.query_selector(Selectors.ebay_tot_rev_span)
                    tot_rev_txt = await tot_rev_span.inner_text()
                    index_fdb = tot_rev_txt.find("Feedback")
                    total_rev = re.sub('[^0-9]', '', tot_rev_txt[:index_fdb - 1])
                    entities['total reviews'] = int(total_rev)
                except Exception as exception:
                    logger.info(exception)
                    self.status = "Error Getting General Stats"
                    self.status_dict['status'] = self.status
                    data_dict[job_id] = self.status_dict
                    return self.status
                else:
                    try:
                        seller_rev_btn = await page.query_selector_all(Selectors.ebay_seller_rev_btn)
                        for i in range(len(seller_rev_btn)):
                            seller_rev_span = await seller_rev_btn[i].query_selector("span")
                            seller_rev_txt = await seller_rev_span.inner_text()
                            if seller_rev_txt == "Received as seller":
                                await seller_rev_btn[i].click()
                                await asyncio.sleep(3)
                    except:
                        pass
                    else:
                        index_view = tot_rev_txt.find('viewing')
                        limit_txt = tot_rev_txt[index_view + 8:len(tot_rev_txt) - 1]
                        index_hyph = limit_txt.find("-")
                        start_txt = limit_txt[:index_hyph]
                        start = int(start_txt)
                        end_txt = limit_txt[index_hyph + 1:]
                        end = int(end_txt)
                        loop = end + 2 - start
                        try:
                            logger.info("SCRAPING REVIEWS..")
                            while True:
                                rev_div = await page.query_selector(Selectors.ebay_rev_div)
                                rate_divs = await rev_div.query_selector_all(Selectors.ebay_rate_divs)
                                rate_count = 0
                                for i in range(1, loop):
                                    name_cls = 'span[data-test-id="fdbk-context-' + str(i) + '"]'
                                    date_cls = 'span[data-test-id="fdbk-time-' + str(i) + '"]'
                                    expr_cls = 'span[data-test-id="fdbk-comment-' + str(i) + '"]'
                                    name_div = await rev_div.query_selector(name_cls)
                                    date_div = await rev_div.query_selector(date_cls)
                                    expr_div = await rev_div.query_selector(expr_cls)
                                    review = {'name': await name_div.inner_text()}
                                    rate_txt = await rate_divs[rate_count].get_attribute("data-test-type")
                                    if rate_txt == "positive":
                                        review["rate"] = 5
                                    elif rate_txt == "negative":
                                        review["rate"] = 0
                                    else:
                                        review["rate"] = None
                                    review['date'] = await date_div.inner_text()
                                    review['experience'] = await expr_div.inner_text()
                                    review["provider"] = provider
                                    reviews.append(review)
                                    entities['reviews'] = reviews
                                    data_dict[job_id] = entities
                                    rate_count += 1
                                try:
                                    next_btn = await page.query_selector(Selectors.ebay_next_btn)
                                    await next_btn.click()
                                    await asyncio.sleep(3)
                                    tot_rev_span = await page.query_selector(Selectors.ebay_tot_rev_span)
                                    tot_rev_txt = await tot_rev_span.inner_text()
                                    limit_txt = tot_rev_txt[index_view + 8:len(tot_rev_txt) - 1]
                                    index_hyph = limit_txt.find("-")
                                    start_txt = limit_txt[:index_hyph]
                                    start = int(start_txt)
                                    end_txt = limit_txt[index_hyph + 1:]
                                    end = int(end_txt)
                                    loop = end + 2 - start
                                    logger.info("REDIRECTING TO NEXT PAGE..")
                                    logger.info("SCRAPING REVIEWS OF NEXT PAGE..")
                                except:
                                    next_btn = None
                                if len(reviews) >= 100 or next_btn is None:
                                    break
                        except Exception as exception:
                            logger.info(exception)
                            self.status = "An Error Occurred During Getting Reviews"
                            self.status_dict['status'] = self.status
                            data_dict[job_id] = self.status_dict
                            return self.status
                        else:
                            logger.info("SCRAPING DONE")
                            logger.info("REVIEWS SCRAPED = %s", len(reviews))
                            entities['reviews'] = reviews

            finally:
                await context.close()
                logger.info("CONTEXT CLOSED")
                await browser.close()
                logger.info("BROWSER CLOSED")
            data_dict[job_id] = entities
            return data_dict

    async def get_etsy_reviews(self, link, data_dict, job_id):
        async with async_playwright() as p:
            try:
                browser = await p.chromium.launch(headless=True)
                logger.info("BROWSER LAUNCHED..")
                context = await browser.new_context(locale='en-GB', bypass_csp=True)

                provider = 'Etsy'
                created_at = datetime.datetime.now()
                expiration = created_at + timedelta.Timedelta(hours=self.hrs)
                expiration = expiration.isoformat()
                updated_at = created_at.isoformat()
                created_at = created_at.isoformat()
                new_uuid = job_id
                reviews = []

                page = await context.new_page()
                logger.info("PAGE CREATED..")

                page.set_default_navigation_timeout(timeout=0)
                await page.goto(link, timeout=0)
                logger.info("NAVIGATED TO LINK.. %s", link)
            except Exception as exception:
                logger.info(exception)
                self.status = "Browser Couldn't Start"
                self.status_dict['status'] = self.status
                data_dict[job_id] = self.status_dict
                return self.status
            else:
                entities = {'provider': provider, 'keyword': link, 'link': page.url, 'uuid': new_uuid,
                            'expiration': str(expiration), 'created_at': str(created_at), 'updated_at': str(updated_at)}
                data_dict[job_id] = entities
                await page.keyboard.press("End")

                logger.info("CHECKING REVIEWS AVAILABILITY..")
                check = await page.query_selector(Selectors.etsy_check)
                if check is None:
                    self.status = "No Reviews Found"
                    self.status_dict['status'] = self.status
                    data_dict[job_id] = self.status_dict
                    return self.status
                try:
                    logger.info("GETTING GENERAL STATS..")
                    stats_div = await page.query_selector(Selectors.etsy_stats_div)
                    gen_rating_div = await stats_div.query_selector(Selectors.etsy_gen_rating_div)
                    gen_rating_txt = await gen_rating_div.inner_text()
                    gen_rating = gen_rating_txt[0]
                    tot_rev_div = await stats_div.query_selector_all(Selectors.etsy_tot_rev_div)
                    tot_rev_txt = await tot_rev_div[2].inner_text()
                    total_rev = re.sub('[^0-9]', '', tot_rev_txt)

                    entities['general rating'] = float(gen_rating)
                    entities['total reviews'] = int(total_rev)
                except Exception as exception:
                    logger.info(exception)
                    self.status = "Error Getting General Stats"
                    self.status_dict['status'] = self.status
                    data_dict[job_id] = self.status_dict
                    return self.status
                else:
                    try:
                        logger.info("SCRAPING REVIEWS..")
                        while True:
                            rev_div = await page.query_selector(Selectors.etsy_rev_div)
                            imgs_div = await rev_div.query_selector_all(Selectors.etsy_imgs_div)
                            names_div = await rev_div.query_selector_all(Selectors.etsy_names_div)
                            rates_div = await rev_div.query_selector_all(Selectors.etsy_rates_div)
                            dates_div = await rev_div.query_selector_all(Selectors.etsy_dates_div)
                            exprs_div = await rev_div.query_selector_all(Selectors.etsy_exprs_div)
                            for i in range(len(names_div)):
                                review = {'image': await imgs_div[i].get_attribute('src'),
                                          'name': await names_div[i].inner_text()}
                                rate_txt = await rates_div[i].inner_text()
                                review['rate'] = float(rate_txt[0])
                                date_txt = await dates_div[i].inner_text()
                                date_idx = date_txt.find(" on ")
                                review['date'] = date_txt[date_idx + 4:]
                                try:
                                    review['experience'] = await exprs_div[i].inner_text()
                                except:
                                    review['experience'] = None
                                review["provider"] = provider
                                reviews.append(review)
                                entities['reviews'] = reviews
                                data_dict[job_id] = entities
                            try:
                                await page.mouse.wheel(0, 5500)
                                nav_div = await page.query_selector(Selectors.etsy_nav_div)
                                if nav_div is not None:
                                    nav = await page.query_selector_all(Selectors.etsy_nav)
                                    next_btn = nav[len(nav) - 1]
                                    await next_btn.click()
                                    await asyncio.sleep(5)
                                    logger.info("REDIRECTING TO NEXT PAGE..")
                                    logger.info("SCRAPING REVIEWS OF NEXT PAGE..")
                                else:
                                    next_btn = None
                            except:
                                next_btn = None
                            if len(reviews) >= 100 or next_btn is None:
                                break
                    except Exception as exception:
                        logger.info(exception)
                        self.status = "An Error Occurred During Getting Reviews"
                        self.status_dict['status'] = self.status
                        data_dict[job_id] = self.status_dict
                        return self.status
                    else:
                        logger.info("SCRAPING DONE")
                        logger.info("REVIEWS SCRAPED = %s", len(reviews))
                        entities['reviews'] = reviews
            finally:
                await context.close()
                logger.info("CONTEXT CLOSED")
                await browser.close()
                logger.info("BROWSER CLOSED")
            data_dict[job_id] = entities
            return data_dict

    async def get_play_store_reviews(self, link, data_dict, job_id):
        async with async_playwright() as p:
            try:
                browser = await p.chromium.launch(headless=True)
                logger.info("BROWSER LAUNCHED..")
                context = await browser.new_context(locale='en-GB', bypass_csp=True)

                provider = 'PlayStore'
                created_at = datetime.datetime.now()
                expiration = created_at + timedelta.Timedelta(hours=self.hrs)
                expiration = expiration.isoformat()
                updated_at = created_at.isoformat()
                created_at = created_at.isoformat()
                new_uuid = job_id
                reviews = []

                page = await context.new_page()
                logger.info("PAGE CREATED..")

                page.set_default_navigation_timeout(timeout=0)
                await page.goto(link, timeout=0)
                logger.info("NAVIGATED TO LINK.. %s", link)
            except Exception as exception:
                logger.info(exception)
                self.status = "Browser Couldn't Start"
                self.status_dict['status'] = self.status
                data_dict[job_id] = self.status_dict
                return self.status
            else:
                entities = {'provider': provider, 'keyword': link, 'link': page.url, 'uuid': new_uuid,
                            'expiration': str(expiration), 'created_at': str(created_at), 'updated_at': str(updated_at)}
                data_dict[job_id] = entities
                logger.info("CHECKING REVIEWS AVAILABILITY..")
                check = await page.query_selector(Selectors.play_store_check)
                if check is None:
                    self.status = "No Reviews Found"
                    self.status_dict['status'] = self.status
                    data_dict[job_id] = self.status_dict
                    return self.status
                try:
                    logger.info("GETTING GENERAL STATS..")
                    gen_rating_div = await page.query_selector(Selectors.play_store_gen_rating_div)
                    gen_rating = await gen_rating_div.inner_text()
                    tot_rev_div = await page.query_selector(Selectors.play_store_tot_rev_div)
                    tot_rev_txt = await tot_rev_div.inner_text()
                    index = tot_rev_txt.find("reviews")
                    tot_rev_k = tot_rev_txt[:index]
                    tot_rev_stp = tot_rev_k.strip()
                    if tot_rev_stp[-1] == 'K':
                        total_rev = tot_rev_stp[:len(tot_rev_stp) - 1]
                        entities['total reviews'] = float(total_rev) * 1000
                    else:
                        total_rev = tot_rev_stp
                        entities['total reviews'] = float(total_rev)
                    entities['general rating'] = float(gen_rating)

                except Exception as exception:
                    logger.info(exception)
                    self.status = "Error Getting General Stats"
                    self.status_dict['status'] = self.status
                    data_dict[job_id] = self.status_dict
                    return self.status
                else:
                    try:
                        logger.info("LOADING ALL REVIEWS..")
                        btn_divs = await page.query_selector_all(Selectors.play_store_btn_divs)
                        for i in range(len(btn_divs)):
                            btn_span = await btn_divs[i].query_selector("span")
                            try:
                                btn_txt = await btn_span.inner_text()
                            except:
                                pass
                            else:
                                if btn_txt == "See all reviews":
                                    await btn_divs[i].click()
                    except Exception as exception:
                        logger.info(exception)
                        self.status = "Error Loading Reviews"
                        self.status_dict['status'] = self.status
                        data_dict[job_id] = self.status_dict
                        return self.status
                    else:
                        try:
                            logger.info("PERFORMING SCROLL..")
                            popup = await page.query_selector(Selectors.play_store_popup)
                            await popup.hover()
                            start_time = time.time()
                            while True:
                                rev_div = await popup.query_selector_all(Selectors.play_store_rev_div)
                                await page.mouse.wheel(0, 2000)
                                await asyncio.sleep(3)
                                current_time = time.time()
                                timeout = current_time - start_time >= 60
                                if len(rev_div) >= 100 or len(rev_div) >= int(entities['total reviews']) or timeout:
                                    break
                        except Exception as exception:
                            logger.info(exception)
                            self.status = "Error Performing Scroll"
                            self.status_dict['status'] = self.status
                            data_dict[job_id] = self.status_dict
                            return self.status
                        else:
                            try:
                                logger.info("SCRAPING REVIEWS..")
                                imgs_div = await popup.query_selector_all(Selectors.play_store_imgs_div)
                                names_div = await popup.query_selector_all(Selectors.play_store_names_div)
                                rates_div = await popup.query_selector_all(Selectors.play_store_rates_div)
                                dates_div = await popup.query_selector_all(Selectors.play_store_dates_div)
                                exprs_div = await popup.query_selector_all(Selectors.play_store_exprs_div)
                                for i in range(len(names_div)):
                                    review = {'image': await imgs_div[i].get_attribute('src'),
                                              'name': await names_div[i].inner_text()}
                                    rate_txt = await rates_div[i].get_attribute('aria-label')
                                    review['rate'] = float(re.sub("[^0-9]", '', rate_txt))
                                    review['date'] = await dates_div[i].inner_text()
                                    try:
                                        review['experience'] = await exprs_div[i].inner_text()
                                    except:
                                        review['experience'] = None
                                    review["provider"] = provider
                                    reviews.append(review)
                                    entities['reviews'] = reviews
                                    data_dict[job_id] = entities
                            except Exception as exception:
                                logger.info(exception)
                                self.status = "An Error Occurred During Getting Reviews"
                                self.status_dict['status'] = self.status
                                data_dict[job_id] = self.status_dict
                                return self.status
                            else:
                                logger.info("SCRAPING DONE")
                                logger.info("REVIEWS SCRAPED = %s", len(reviews))
                                entities['reviews'] = reviews
            finally:
                await context.close()
                logger.info("CONTEXT CLOSED")
                await browser.close()
                logger.info("BROWSER CLOSED")
            data_dict[job_id] = entities
            return data_dict

    async def scrape_trip_advisor_attraction_reviews(self, page, data_dict, job_id):
        scraped_reviews = []
        logger.info("CHECKING AVAILABILITY OF REVIEWS..")
        check = await page.query_selector(Selectors.trip_advisor_attract_check)
        if check is None:
            self.status = "No Reviews Found"
            self.status_dict['status'] = self.status
            data_dict[job_id] = self.status_dict
            return self.status
        try:
            logger.info("GETTING GENERAL STATS..")
            stats_div = await page.query_selector(Selectors.trip_advisor_attract_stats_div)
            gen_rate_div = await stats_div.query_selector(Selectors.trip_advisor_attract_gen_rate_div)
            gen_rating = await gen_rate_div.inner_text()
            tot_rev_div = await stats_div.query_selector(Selectors.trip_advisor_attract_tot_rev_div)
            tot_rev_txt = await tot_rev_div.inner_text()
            total_rev = re.sub('[^0-9]', '', tot_rev_txt)

            self.entities['general rating'] = float(gen_rating)
            self.entities['total reviews'] = int(total_rev)
        except Exception as exception:
            logger.info(exception)
            self.resp_type = "status"
            self.status = "Error Getting General Stats"
            self.status_dict['status'] = self.status
            data_dict[job_id] = self.status_dict
            return self.status
        else:
            try:
                await asyncio.sleep(2)
                logger.info("SETTING LANGUAGE..")
                selection_div = await page.query_selector(Selectors.trip_advisor_attract_selection_div)
                selection = await selection_div.query_selector_all(Selectors.trip_advisor_attract_selection)
                for j in range(len(selection)):
                    select_span = await selection[j].query_selector(Selectors.trip_advisor_attract_select_span)
                    try:
                        span_txt = await select_span.inner_text()
                    except:
                        span_txt = None
                    if span_txt == 'English':
                        await selection[j].click()
                        dropdown = await page.query_selector(Selectors.trip_advisor_attract_dropdown)
                        await dropdown.hover()
                        drop_li = await dropdown.query_selector_all('li')
                        await drop_li[len(drop_li) - 1].click()
            except Exception as exception:
                logger.info(exception)
                self.resp_type = "status"
                self.status = "Error Setting Language"
                self.status_dict['status'] = self.status
                data_dict[job_id] = self.status_dict
                return self.status
            else:
                try:
                    logger.info("SCRAPING REVIEWS..")
                    while True:
                        await page.mouse.wheel(0, 5000)
                        await asyncio.sleep(1)
                        outer_div = await page.query_selector(Selectors.trip_advisor_attract_outer_div)
                        rev_div = await outer_div.query_selector_all(Selectors.trip_advisor_attract_rev_div)
                        for i in range(len(rev_div)):
                            pics_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_attract_pics_div)
                            names_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_attract_names_div)
                            rates_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_attract_rates_div)
                            titles_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_attract_titles_div)
                            dates_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_attract_dates_div)
                            exprs_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_attract_exprs_div)
                            source_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_attract_source_div)
                            review = {}
                            img_div = await pics_div[0].query_selector("img")
                            review['image'] = await img_div.get_attribute('src')
                            review['name'] = await names_div[0].inner_text()
                            rate_txt = await rates_div[0].get_attribute('aria-label')
                            review['rate'] = float(rate_txt[0])
                            date_txt = await dates_div[0].inner_text()
                            index = date_txt.find("Written")
                            review['date'] = date_txt[index + 8:]
                            review['title'] = await titles_div[0].inner_text()
                            try:
                                review['experience'] = await exprs_div[1].inner_text()
                            except:
                                review['experience'] = None
                            try:
                                review['source'] = "https://www.tripadvisor.com" + await source_div[1].get_attribute(
                                    'href')
                            except:
                                review['source'] = None
                            review["provider"] = "TripAdvisor"
                            scraped_reviews.append(review)
                            data_dict[job_id]['reviews'] = scraped_reviews
                        try:
                            next_btn = await page.query_selector(Selectors.trip_advisor_attract_next_btn)
                            await next_btn.click()
                            await asyncio.sleep(5)
                            logger.info("REDIRECTING TO NEXT PAGE..")
                            logger.info("SCRAPING REVIEWS OF NEXT PAGE..")
                        except:
                            next_btn = None
                        if len(scraped_reviews) >= 100 or next_btn is None:
                            break
                except Exception as exception:
                    logger.info(exception)
                    self.resp_type = "status"
                    self.status = "An Error Occurred During Getting Reviews"
                    self.status_dict['status'] = self.status
                    data_dict[job_id] = self.status_dict
                    return self.status
                else:
                    logger.info("SCRAPING DONE.")
                    logger.info("REVIEWS SCRAPED = %s", len(scraped_reviews))
        return scraped_reviews

    async def scrape_trip_advisor_airline_reviews(self, page, data_dict, job_id):
        scraped_reviews = []
        logger.info("CHECKING AVAILABILITY OF REVIEWS..")
        check = await page.query_selector(Selectors.trip_advisor_airline_check)
        if check is None:
            self.status = "No Reviews Found"
            self.status_dict['status'] = self.status
            data_dict[job_id] = self.status_dict

            return self.status
        try:
            logger.info("GETTING GENERAL STATS..")
            stats_div = await page.query_selector(Selectors.trip_advisor_airline_stats_div)
            gen_rate_div = await stats_div.query_selector(Selectors.trip_advisor_airline_gen_rate_div)
            gen_rating = await gen_rate_div.inner_text()
            tot_rev_div = await stats_div.query_selector(Selectors.trip_advisor_airline_tot_rev_div)
            tot_rev_txt = await tot_rev_div.inner_text()
            total_rev = re.sub('[^0-9]', '', tot_rev_txt)

            self.entities['general rating'] = float(gen_rating)
            self.entities['total reviews'] = int(total_rev)
        except Exception as exception:
            logger.info(exception)
            self.resp_type = "status"
            self.status = "Error Getting General Stats"
            self.status_dict['status'] = self.status
            data_dict[job_id] = self.status_dict
            return self.status
        else:
            await asyncio.sleep(2)
            try:
                logger.info("SETTING LANGUAGE..")
                selection = await page.query_selector_all(Selectors.trip_advisor_airline_selection)
                await selection[0].click()
                await asyncio.sleep(2)
            except Exception as exception:
                logger.info(exception)
                self.resp_type = "status"
                self.status = "Error Setting Language"
                self.status_dict['status'] = self.status
                data_dict[job_id] = self.status_dict
                return self.status
            else:
                try:
                    logger.info("SCRAPING REVIEWS..")
                    nb_rev = int(total_rev)
                    while True:
                        calcul = nb_rev - 5
                        if calcul >= 0:
                            loop = 5
                        else:
                            loop = nb_rev
                        await page.mouse.wheel(0, 5000)
                        await asyncio.sleep(1)
                        outer_div = await page.query_selector(Selectors.trip_advisor_airline_outer_div)
                        rev_div = await outer_div.query_selector_all(Selectors.trip_advisor_airline_rev_div)

                        for i in range(loop):
                            pics_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_airline_pics_div)
                            names_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_airline_names_div)
                            rates_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_airline_rates_div)
                            dates_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_airline_dates_div)
                            titles_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_airline_titles_div)
                            exprs_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_airline_exprs_div)
                            review = {}
                            img_div = await pics_div[0].query_selector("img")
                            review['image'] = await img_div.get_attribute('src')
                            review['name'] = await names_div[0].inner_text()
                            rate_span = await rates_div[0].query_selector("span")
                            rate_txt = await rate_span.get_attribute('class')
                            review['rate'] = float(re.sub('[^0-9]', '', rate_txt)) / 10
                            date_span = await dates_div[0].query_selector("span")
                            date_txt = await date_span.inner_text()
                            index = date_txt.find("review")
                            review['date'] = date_txt[index + 7:]
                            title_span = await titles_div[0].query_selector_all("span")
                            review['title'] = await title_span[0].inner_text()
                            try:
                                expr_span = await exprs_div[0].query_selector("span")
                                review['experience'] = await expr_span.inner_text()
                            except:
                                review['experience'] = None
                            try:
                                review['source'] = "https://www.tripadvisor.com" + await titles_div[0].get_attribute(
                                    'href')
                            except:
                                review['source'] = None
                            review["provider"] = "TripAdvisor"
                            scraped_reviews.append(review)
                            data_dict[job_id]['reviews'] = scraped_reviews
                        try:
                            next_btn = await page.query_selector(Selectors.trip_advisor_airline_next_btn)
                            await next_btn.click()
                            await asyncio.sleep(5)
                            logger.info("REDIRECTING TO NEXT PAGE..")
                            logger.info("SCRAPING REVIEWS OF NEXT PAGE..")
                            nb_rev = calcul
                        except:
                            next_btn = None
                        if len(scraped_reviews) >= 100 or next_btn is None:
                            break
                except Exception as exception:
                    logger.info(exception)
                    self.resp_type = "status"
                    self.status = "An Error Occurred During Getting Reviews"
                    self.status_dict['status'] = self.status
                    data_dict[job_id] = self.status_dict
                    return self.status
                else:
                    logger.info("SCRAPING DONE.")
                    logger.info("REVIEWS SCRAPED = %s", len(scraped_reviews))
        return scraped_reviews

    async def scrape_trip_advisor_hotel_reviews(self, page, data_dict, job_id):
        scraped_reviews = []
        logger.info("CHECKING AVAILABILITY OF REVIEWS..")
        check = await page.query_selector(Selectors.trip_advisor_hotel_check)
        if check is None:
            self.resp_type = "status"
            self.status = "No Reviews Found"
            self.status_dict['status'] = self.status
            data_dict[job_id] = self.status_dict
            return self.status
        try:
            logger.info("GETTING GENERAL STATS..")
            stats_div = await page.query_selector(Selectors.trip_advisor_hotel_stats_div)
            gen_rate_div = await stats_div.query_selector(Selectors.trip_advisor_hotel_gen_rate_div)
            gen_rating = await gen_rate_div.inner_text()
            tot_rev_div = await stats_div.query_selector(Selectors.trip_advisor_hotel_tot_rev_div)
            tot_rev_txt = await tot_rev_div.inner_text()
            total_rev = re.sub('[^0-9]', '', tot_rev_txt)

            self.entities['general rating'] = float(gen_rating)
            self.entities['total reviews'] = int(total_rev)
        except Exception as exception:
            logger.info(exception)
            self.resp_type = "status"
            self.status = "Error Getting General Stats"
            self.status_dict['status'] = self.status
            data_dict[job_id] = self.status_dict
            return self.status
        else:
            await asyncio.sleep(2)
            try:
                logger.info("SETTING LANGUAGE..")
                selection = await page.query_selector_all(Selectors.trip_advisor_hotel_selection)
                await selection[0].click()
                await asyncio.sleep(2)
            except Exception as exception:
                logger.info(exception)
                self.resp_type = "status"
                self.status = "Error Setting Language"
                self.status_dict['status'] = self.status
                data_dict[job_id] = self.status_dict
                return self.status
            else:
                try:
                    logger.info("SCRAPING REVIEWS..")
                    while True:
                        await page.mouse.wheel(0, 7000)
                        await asyncio.sleep(2)
                        outer_div = await page.query_selector(Selectors.trip_advisor_hotel_outer_div)
                        rev_div = await outer_div.query_selector_all(Selectors.trip_advisor_hotel_rev_div)
                        for i in range(len(rev_div)):
                            pics_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_hotel_pics_div)
                            names_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_hotel_names_div)
                            rates_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_hotel_rates_div)
                            dates_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_hotel_dates_div)
                            titles_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_hotel_titles_div)
                            exprs_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_hotel_exprs_div)
                            review = {}
                            img_div = await pics_div[0].query_selector("img")
                            review['image'] = await img_div.get_attribute('src')
                            review['name'] = await names_div[0].inner_text()
                            rate_span = await rates_div[0].query_selector("span")
                            rate_txt = await rate_span.get_attribute('class')
                            review['rate'] = float(re.sub('[^0-9]', '', rate_txt)) / 10
                            date_span = await dates_div[0].query_selector("span")
                            date_txt = await date_span.inner_text()
                            index = date_txt.find("review")
                            review['date'] = date_txt[index + 7:]
                            title_span = await titles_div[0].query_selector_all("span")
                            review['title'] = await title_span[0].inner_text()
                            try:
                                expr_span = await exprs_div[0].query_selector("span")
                                review['experience'] = await expr_span.inner_text()
                            except:
                                review['experience'] = None
                            try:
                                review['source'] = "https://www.tripadvisor.com" + await titles_div[0].get_attribute(
                                    'href')
                            except:
                                review['source'] = None
                            review["provider"] = "TripAdvisor"
                            scraped_reviews.append(review)
                            data_dict[job_id]['reviews'] = scraped_reviews
                        try:
                            next_btn = await page.query_selector(Selectors.trip_advisor_hotel_next_btn)
                            await next_btn.click()
                            await asyncio.sleep(5)
                            logger.info("REDIRECTING TO NEXT PAGE..")
                            logger.info("SCRAPING REVIEWS OF NEXT PAGE..")
                        except:
                            next_btn = None
                        if len(scraped_reviews) >= 100 or next_btn is None:
                            break
                except Exception as exception:
                    logger.info(exception)
                    self.resp_type = "status"
                    self.status = "An Error Occurred During Getting Reviews"
                    self.status_dict['status'] = self.status
                    data_dict[job_id] = self.status_dict
                    return self.status
                else:
                    logger.info("SCRAPING DONE.")
                    logger.info("REVIEWS SCRAPED = %s", len(scraped_reviews))
        return scraped_reviews

    async def scrape_trip_advisor_restaurant_reviews(self, page, data_dict, job_id):
        scraped_reviews = []
        read_more_clicked = False
        logger.info("CHECKING AVAILABILITY OF REVIEWS..")
        check = await page.query_selector(Selectors.trip_advisor_resto_check)
        if check is None:
            self.resp_type = "status"
            self.status = "No Reviews Found"
            self.status_dict['status'] = self.status
            data_dict[job_id] = self.status_dict
            return self.status
        try:
            logger.info("GETTING GENERAL STATS..")
            stats_div = await page.query_selector(Selectors.trip_advisor_resto_stats_div)
            gen_rate_div = await stats_div.query_selector(Selectors.trip_advisor_resto_gen_rate_div)
            gen_rating = await gen_rate_div.inner_text()
            tot_rev_div = await stats_div.query_selector(Selectors.trip_advisor_resto_tot_rev_div)
            tot_rev_txt = await tot_rev_div.inner_text()
            total_rev = re.sub('[^0-9]', '', tot_rev_txt)

            self.entities['general rating'] = float(gen_rating)
            self.entities['total reviews'] = int(total_rev)
        except Exception as exception:
            logger.info(exception)
            self.resp_type = "status"
            self.status = "Error Getting General Stats"
            self.status_dict['status'] = self.status
            data_dict[job_id] = self.status_dict
            return self.status
        else:
            try:
                logger.info("SETTING LANGUAGE..")
                selection = await page.query_selector_all(Selectors.trip_advisor_resto_selection)
                await selection[0].click()
                await asyncio.sleep(2)
            except Exception as exception:
                logger.info(exception)
                self.resp_type = "status"
                self.status = "Error Setting Language"
                self.status_dict['status'] = self.status
                data_dict[job_id] = self.status_dict
                return self.status
            else:
                try:
                    logger.info("SCRAPING REVIEWS..")
                    while True:
                        await page.keyboard.press("End")
                        outer_div = await page.query_selector(Selectors.trip_advisor_resto_outer_div)
                        rev_div = await outer_div.query_selector_all(Selectors.trip_advisor_resto_rev_div)
                        logger.info(len(rev_div))
                        for i in range(len(rev_div)):
                            if not read_more_clicked:
                                try:
                                    read_more = await rev_div[i].query_selector(Selectors.trip_advisor_resto_read_more)
                                    logger.info(read_more)
                                    await read_more.click()
                                    read_more_clicked = True
                                except:
                                    pass
                            imgs_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_resto_imgs_div)
                            names_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_resto_names_div)
                            rates_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_resto_rates_div)
                            titles_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_resto_titles_div)
                            exprs_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_resto_exprs_div)
                            source_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_resto_source_div)
                            review = {'image': await imgs_div[0].get_attribute('src')}
                            try:
                                name_div = await names_div[0].query_selector("div")
                                review['name'] = await name_div.inner_text()
                            except:
                                members_names_div = await rev_div[i].query_selector_all(
                                    Selectors.trip_advisor_resto_members_names_div)
                                member_name_div = await members_names_div[0].query_selector("div")
                                review['name'] = await member_name_div.inner_text()
                            rate_span = await rates_div[0].query_selector("span")
                            rate_txt = await rate_span.get_attribute('class')
                            review['rate'] = float(re.sub('[^0-9]', '', rate_txt)) / 10
                            review['title'] = await titles_div[0].inner_text()
                            date_span = await rates_div[0].query_selector("span.ratingDate")
                            date_txt = await date_span.inner_text()
                            index = date_txt.find("Reviewed")
                            review['date'] = date_txt[index + 9:]
                            try:
                                review['experience'] = await exprs_div[0].inner_text()
                            except:
                                review['experience'] = None
                            try:
                                review['source'] = "https://www.tripadvisor.com" + await source_div[0].get_attribute(
                                    'href')
                            except:
                                review['source'] = None
                            review["provider"] = "TripAdvisor"
                            scraped_reviews.append(review)
                            data_dict[job_id]['reviews'] = scraped_reviews
                        try:
                            next_btn = await page.query_selector(Selectors.trip_advisor_resto_next_btn)
                            await next_btn.click()
                            await asyncio.sleep(5)
                            logger.info("REDIRECTING TO NEXT PAGE..")
                            logger.info("SCRAPING REVIEWS OF NEXT PAGE..")
                            read_more_clicked = False
                        except:
                            next_btn = None
                        if len(scraped_reviews) >= 100 or next_btn is None:
                            break
                except Exception as exception:
                    logger.info(exception)
                    self.resp_type = "status"
                    self.status = "An Error Occurred During Getting Reviews"
                    self.status_dict['status'] = self.status
                    data_dict[job_id] = self.status_dict
                    return self.status
                else:
                    logger.info("SCRAPING DONE.")
                    logger.info("REVIEWS SCRAPED = %s", len(scraped_reviews))
        return scraped_reviews

    async def scrape_trip_advisor_rental_reviews(self, page, data_dict, job_id):
        scraped_reviews = []
        logger.info("CHECKING AVAILABILITY OF REVIEWS..")
        check_outer_div = await page.query_selector(Selectors.trip_advisor_rental_check_outer_div)
        check = await check_outer_div.query_selector(Selectors.trip_advisor_rental_check)
        if check is None:
            self.resp_type = "status"
            self.status = "No Reviews Found"
            self.status_dict['status'] = self.status
            data_dict[job_id] = self.status_dict
            return self.status
        try:
            logger.info("GETTING GENERAL STATS..")
            stats_div = await page.query_selector(Selectors.trip_advisor_rental_stats_div)
            gen_rate_div = await stats_div.query_selector(Selectors.trip_advisor_rental_gen_rate_div)
            gen_rate_txt = await gen_rate_div.get_attribute('aria-label')
            gen_rating = gen_rate_txt[:3]
            tot_rev_div = await stats_div.query_selector(Selectors.trip_advisor_rental_tot_rev_div)
            tot_rev_txt = await tot_rev_div.inner_text()
            total_rev = re.sub('[^0-9]', '', tot_rev_txt)

            self.entities['general rating'] = float(gen_rating)
            self.entities['total reviews'] = int(total_rev)
        except Exception as exception:
            logger.info(exception)
            self.resp_type = "status"
            self.status = "Error Getting General Stats"
            self.status_dict['status'] = self.status
            data_dict[job_id] = self.status_dict
            return self.status
        else:
            try:
                logger.info("SCRAPING REVIEWS..")
                while True:
                    outer_div = await page.query_selector(Selectors.trip_advisor_rental_outer_div)
                    rev_div = await outer_div.query_selector_all(Selectors.trip_advisor_rental_rev_div)
                    for i in range(len(rev_div)):
                        pics_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_rental_pics_div)
                        names_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_rental_names_div)
                        rates_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_rental_rates_div)
                        titles_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_rental_titles_div)
                        dates_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_rental_dates_div)
                        exprs_div = await rev_div[i].query_selector_all(Selectors.trip_advisor_rental_exprs_div)
                        try:
                            review = {}
                            img_div = await pics_div[0].query_selector("img")
                            review['image'] = await img_div.get_attribute('src')
                            review['name'] = await names_div[0].inner_text()
                            rate_txt = await rates_div[0].get_attribute('aria-label')
                            review['rate'] = float(rate_txt[:3])
                            date_txt = await dates_div[0].inner_text()
                            index_start = date_txt.find("Written")
                            index_end = date_txt.find(".")
                            review['date'] = date_txt[index_start + 8:index_end]
                            review['title'] = await titles_div[0].inner_text()
                            try:
                                review['experience'] = await exprs_div[0].inner_text()
                            except:
                                review['experience'] = None
                            try:
                                review['source'] = "https://www.tripadvisor.com" + await names_div[0].get_attribute(
                                    'href')
                            except:
                                review['source'] = None
                            review["provider"] = "TripAdvisor"
                            scraped_reviews.append(review)
                            data_dict[job_id]['reviews'] = scraped_reviews
                        except:
                            continue
                    try:
                        next_btn = await page.query_selector(Selectors.trip_advisor_rental_next_btn)
                        await next_btn.click()
                        await asyncio.sleep(3)
                        logger.info("REDIRECTING TO NEXT PAGE..")
                        logger.info("SCRAPING REVIEWS OF NEXT PAGE..")
                    except:
                        next_btn = None
                    if len(scraped_reviews) >= 100 or next_btn is None:
                        break
            except Exception as exception:
                logger.info(exception)
                self.resp_type = "status"
                self.status = "An Error Occurred During Getting Reviews"
                self.status_dict['status'] = self.status
                data_dict[job_id] = self.status_dict
                return self.status
            else:
                logger.info("SCRAPING DONE.")
                logger.info("REVIEWS SCRAPED = %s", len(scraped_reviews))
        return scraped_reviews

    async def get_trip_advisor_reviews(self, link, data_dict, job_id):
        logger.info("------- ENTERED TRIP ADVISOR REVIEWS FUNCTION -------")
        async with async_playwright() as p:
            try:
                browser = await p.chromium.launch(headless=True)
                logger.info("BROWSER LAUNCHED..")
                context = await browser.new_context(locale='en-GB', bypass_csp=True)

                provider = 'TripAdvisor'
                created_at = datetime.datetime.now()
                expiration = created_at + timedelta.Timedelta(hours=self.hrs)
                expiration = expiration.isoformat()
                updated_at = created_at.isoformat()
                created_at = created_at.isoformat()
                new_uuid = job_id
                response = []

                page = await context.new_page()
                logger.info("PAGE CREATED..")

                page.set_default_navigation_timeout(timeout=0)
                await page.goto(link, timeout=0)
                logger.info("NAVIGATED TO LINK.. %s", link)
            except Exception as exception:
                logger.info(exception)
                self.status = "Browser Couldn't Start"
                self.status_dict['status'] = self.status
                data_dict[job_id] = self.status_dict
                return self.status
            else:
                self.entities['provider'] = provider
                self.entities['link'] = page.url
                self.entities['keyword'] = page.url
                self.entities['uuid'] = new_uuid
                self.entities['expiration'] = str(expiration)
                self.entities['created_at'] = str(created_at)
                self.entities['updated_at'] = str(updated_at)
                data_dict[job_id] = self.entities

                logger.info("REDIRECTING TO SCRAPING FUNCTION..")
                if link.find("Attraction") != -1:
                    response = await self.scrape_trip_advisor_attraction_reviews(page, data_dict, job_id)
                elif link.find("Hotel") != -1:
                    response = await self.scrape_trip_advisor_hotel_reviews(page, data_dict, job_id)
                elif link.find("Airline") != -1:
                    response = await self.scrape_trip_advisor_airline_reviews(page, data_dict, job_id)
                elif link.find("Restaurant") != -1:
                    response = await self.scrape_trip_advisor_restaurant_reviews(page, data_dict, job_id)
                elif link.find("Rental") != -1:
                    response = await self.scrape_trip_advisor_rental_reviews(page, data_dict, job_id)
                else:
                    self.status = "Site Structure Not Handled"
                    self.status_dict['status'] = self.status
                    data_dict[job_id] = self.status_dict
                    return self.status
            finally:
                await context.close()
                logger.info("CONTEXT CLOSED")
                await browser.close()
                logger.info("BROWSER CLOSED")
                if self.resp_type != "status":
                    self.entities['reviews'] = response
                    data_dict[job_id] = self.entities
            return data_dict

    async def scrape_capterra_service_reviews(self, page, data_dict, job_id):
        scraped_reviews = []
        try:
            logger.info("GETTING GENERAL STATS..")
            stats_div = await page.query_selector(Selectors.capterra_service_stats_div)
            stats_txt = await stats_div.inner_text()
            gen_rating = stats_txt[:3]
            tot_rev_txt = stats_txt[4:]
            total_rev = re.sub('[^0-9]', '', tot_rev_txt)

            logger.info("CHECKING AVAILABILITY OF REVIEWS..")
            if int(total_rev) == 0:
                self.resp_type = "status"
                self.status = "No Reviews Found"
                self.status_dict['status'] = self.status
                data_dict[job_id] = self.status_dict
                return self.status

            else:
                self.entities['general rating'] = float(gen_rating)
                self.entities['total reviews'] = int(total_rev)
        except Exception as exception:
            logger.info(exception)
            self.resp_type = "status"
            self.status = "Error Getting General Stats"
            self.status_dict['status'] = self.status
            data_dict[job_id] = self.status_dict
            return self.status
        else:
            try:
                logger.info("SCRAPING REVIEWS..")
                await page.keyboard.press("End")
                await asyncio.sleep(2)
                while True:
                    rev_div = await page.query_selector_all(Selectors.capterra_service_rev_div)
                    for j in range(len(rev_div)):
                        imgs_div = await rev_div[j].query_selector_all(Selectors.capterra_service_imgs_div)
                        names_div = await rev_div[j].query_selector_all(Selectors.capterra_service_names_div)
                        posts_div = await rev_div[j].query_selector_all(Selectors.capterra_service_posts_div)
                        rates_div = await rev_div[j].query_selector_all(Selectors.capterra_service_rates_div)
                        exprs_div = await rev_div[j].query_selector_all(Selectors.capterra_service_exprs_div)

                        review = {}
                        try:
                            review['image'] = await imgs_div[0].get_attribute('src')
                        except:
                            review['image'] = None
                        review['name'] = await names_div[0].inner_text()
                        if len(posts_div) == 2:
                            post0 = await posts_div[0].inner_text()
                            post1 = await posts_div[1].inner_text()
                            ch_post = post0 + "\n" + post1
                            review['post'] = ch_post
                        elif len(posts_div) == 1:
                            review['post'] = await posts_div[0].inner_text()
                        else:
                            review['post'] = None
                        review['rate'] = await rates_div[0].inner_text()
                        if len(exprs_div) == 2:
                            expr_span = await exprs_div[1].query_selector(Selectors.capterra_service_expr_span)
                        else:
                            expr_span = await exprs_div[0].query_selector(Selectors.capterra_service_expr_span)
                        review['experience'] = await expr_span.inner_text()
                        review["provider"] = "Capterra"
                        scraped_reviews.append(review)
                    if len(scraped_reviews) >= 10:
                        break
            except Exception as exception:
                logger.info(exception)
                self.resp_type = "status"
                self.status = "An Error Occurred During Getting Reviews"
                self.status_dict['status'] = self.status
                data_dict[job_id] = self.status_dict
                return self.status
        return scraped_reviews

    async def scrape_capterra_other_reviews(self, page, data_dict, job_id):
        scraped_reviews = []
        try:
            logger.info("GETTING GENERAL STATS..")
            outer_stats_div = await page.query_selector(Selectors.capterra_other_outer_stats_div)
            stats_div = await outer_stats_div.query_selector(Selectors.capterra_other_stats_div)
            stats_txt = await stats_div.inner_text()
            gen_rating = stats_txt[:3]
            tot_rev_txt = stats_txt[4:]
            total_rev = re.sub('[^0-9]', '', tot_rev_txt)

            logger.info("CHECKING AVAILABILITY OF REVIEWS..")
            if int(total_rev) == 0:
                self.resp_type = "status"
                self.status = "No Reviews Found"
                self.status_dict['status'] = self.status
                data_dict[job_id] = self.status_dict
                return self.status
            else:
                self.entities['general rating'] = float(gen_rating)
                self.entities['total reviews'] = int(total_rev)
        except Exception as exception:
            logger.info(exception)
            self.resp_type = "status"
            self.status = "Error Getting General Stats"
            self.status_dict['status'] = self.status
            data_dict[job_id] = self.status_dict
            return self.status
        else:
            try:
                logger.info("SCRAPING REVIEWS..")
                await page.keyboard.press("End")
                await asyncio.sleep(2)
                while True:
                    rev_div = await page.query_selector_all(Selectors.capterra_other_rev_div)
                    for j in range(len(rev_div)):
                        imgs_div = await rev_div[j].query_selector_all(Selectors.capterra_other_imgs_div)
                        names_div = await rev_div[j].query_selector_all(Selectors.capterra_other_names_div)
                        posts_div = await rev_div[j].query_selector_all(Selectors.capterra_other_posts_div)
                        dates_div = await rev_div[j].query_selector_all(Selectors.capterra_other_dates_div)
                        titles_div = await rev_div[j].query_selector_all(Selectors.capterra_other_titles_div)
                        exprs_div = await rev_div[j].query_selector_all(Selectors.capterra_other_exprs_div)

                        review = {}
                        try:
                            review['image'] = await imgs_div[0].get_attribute('src')
                        except:
                            review['image'] = None
                        review['name'] = await names_div[0].inner_text()
                        if len(posts_div) >= 2:
                            post0 = await posts_div[0].inner_text()
                            post1 = await posts_div[1].inner_text()
                            ch_post = post0 + "\n" + post1
                            review['post'] = ch_post
                        elif len(posts_div) == 1:
                            review['post'] = await posts_div[0].inner_text()
                        else:
                            review['post'] = None
                        review['date'] = await dates_div[2].inner_text()
                        review['title'] = await titles_div[0].inner_text()
                        review['experience'] = await exprs_div[0].inner_text()
                        review["provider"] = "Capterra"
                        scraped_reviews.append(review)

                    if len(scraped_reviews) >= 20:
                        break
            except Exception as exception:
                logger.info(exception)
                self.resp_type = "status"
                self.status = "An Error Occurred During Getting Reviews"
                self.status_dict['status'] = self.status
                data_dict[job_id] = self.status_dict
                return self.status
        return scraped_reviews

    async def get_capterra_reviews(self, link, data_dict, job_id):
        async with async_playwright() as p:
            try:
                browser = await p.chromium.launch(headless=True)
                logger.info("BROWSER LAUNCHED..")
                context = await browser.new_context(locale='en-GB', bypass_csp=True, user_agent=self.useragent)

                provider = 'Capterra'
                created_at = datetime.datetime.now()
                expiration = created_at + timedelta.Timedelta(hours=self.hrs)
                expiration = expiration.isoformat()
                updated_at = created_at.isoformat()
                created_at = created_at.isoformat()
                new_uuid = job_id

                page = await context.new_page()
                logger.info("PAGE CREATED..")

                page.set_default_navigation_timeout(timeout=0)
                await page.goto(link, timeout=0)
                logger.info("NAVIGATED TO LINK.. %s", link)
                await asyncio.sleep(5)
            except Exception as exception:
                logger.info(exception)
                self.status = "Browser Couldn't Start"
                self.status_dict['status'] = self.status
                data_dict[job_id] = self.status_dict
                return self.status
            else:
                self.entities['provider'] = provider
                self.entities['keyword'] = link
                self.entities['link'] = page.url
                self.entities['uuid'] = new_uuid
                self.entities['expiration'] = str(expiration)
                self.entities['created_at'] = str(created_at)
                self.entities['updated_at'] = str(updated_at)

                logger.info("HANDLING POPUP")
                popup = await page.query_selector("div.sb.color-mode-light.card-content")
                logger.info(popup)
                # Code..

                logger.info("REDIRECTING TO SCRAPING FUNCTION..")
                if link.find("services") != -1:
                    response = await self.scrape_capterra_service_reviews(page, data_dict, job_id)
                else:
                    response = await self.scrape_capterra_other_reviews(page, data_dict, job_id)
            finally:
                await context.close()
                logger.info("CONTEXT CLOSED")
                await browser.close()
                logger.info("BROWSER CLOSED")
            if self.resp_type != "status":
                self.entities['reviews'] = response
                data_dict[job_id] = self.entities
            return data_dict

    async def get_g2_reviews(self, link, data_dict, job_id):
        async with async_playwright() as p:
            try:
                browser = await p.chromium.launch(headless=True)
                logger.info("BROWSER LAUNCHED..")
                context = await browser.new_context(locale='en-GB', user_agent=self.useragent)

                provider = 'G2'
                created_at = datetime.datetime.now()
                expiration = created_at + timedelta.Timedelta(hours=self.hrs)
                expiration = expiration.isoformat()
                updated_at = created_at.isoformat()
                created_at = created_at.isoformat()
                new_uuid = job_id
                reviews = []

                page = await context.new_page()
                logger.info("PAGE CREATED..")

                page.set_default_navigation_timeout(timeout=0)
                await page.goto(link, timeout=0)
                logger.info("NAVIGATED TO LINK.. %s", link)
                await asyncio.sleep(5)
            except Exception as exception:
                logger.info(exception)
                self.status = "Browser Couldn't Start"
                self.status_dict['status'] = self.status
                data_dict[job_id] = self.status_dict
                return self.status
            else:
                try:
                    logger.info("HANDLING VERIFICATIONS..")
                    iframe_elt = await page.query_selector('iframe')
                    iframe = await iframe_elt.content_frame()
                    await iframe.wait_for_selector(Selectors.g2_check_box)
                    check_box = await iframe.query_selector(Selectors.g2_check_box)
                    await check_box.check()
                    await asyncio.sleep(5)
                    iframe_elt = await page.query_selector('iframe')
                    if iframe_elt is not None:
                        iframe = await iframe_elt.content_frame()
                        await iframe.wait_for_selector(Selectors.g2_check_box)
                        check_box = await iframe.query_selector(Selectors.g2_check_box)
                        await check_box.check()
                        await asyncio.sleep(5)
                except Exception as exception:
                    logger.info(exception)
                    self.status = "Handling Verification Failed"
                    self.status_dict['status'] = self.status
                    data_dict[job_id] = self.status_dict
                    return self.status
                else:
                    entities = {'provider': provider, 'keyword': link, 'link': page.url, 'uuid': new_uuid,
                                'expiration': str(expiration), 'created_at': str(created_at),
                                'updated_at': str(updated_at)}
                    data_dict[job_id] = entities

                    logger.info("CHECKING REVIEWS AVAILABILITY..")
                    check = await page.query_selector(Selectors.g2_check)
                    if check is None:
                        self.status = "No Reviews Found"
                        self.status_dict['status'] = self.status
                        data_dict[job_id] = self.status_dict
                        return self.status
                    try:
                        logger.info("GETTING GENERAL STATS..")
                        stats_div = await page.query_selector(Selectors.g2_stats_div)
                        gen_rate_div = await stats_div.query_selector(Selectors.g2_gen_rate_div)
                        gen_rate_txt = await gen_rate_div.get_attribute("class")
                        gen_rating = re.sub('[^0-9]', '', gen_rate_txt[len(gen_rate_txt) - 1])
                        tot_rev_div = await page.query_selector(Selectors.g2_tot_rev_div)
                        tot_rev_txt = await tot_rev_div.inner_text()
                        total_rev = re.sub('[^0-9]', '', tot_rev_txt)

                        entities['general rating'] = float(gen_rating) // 2
                        entities['total reviews'] = int(total_rev)
                    except Exception as exception:
                        logger.info(exception)
                        self.status = "Error Getting General Stats"
                        self.status_dict['status'] = self.status
                        data_dict[job_id] = self.status_dict
                        return self.status
                    else:
                        try:
                            logger.info("SCRAPING REVIEWS..")
                            while True:
                                expr_ch = ""
                                rev_div = await page.query_selector_all(Selectors.g2_rev_div)
                                for j in range(len(rev_div)):
                                    imgs_div = await rev_div[j].query_selector_all(Selectors.g2_imgs_div)
                                    names_div = await rev_div[j].query_selector_all(Selectors.g2_names_div)
                                    posts_div = await rev_div[j].query_selector_all(Selectors.g2_posts_div)
                                    rates_div = await rev_div[j].query_selector_all(Selectors.g2_rates_div)
                                    dates_div = await rev_div[j].query_selector_all(Selectors.g2_dates_div)
                                    titles_div = await rev_div[j].query_selector_all(Selectors.g2_titles_div)
                                    exprs_div = await rev_div[j].query_selector_all(Selectors.g2_exprs_div)

                                    review = {}
                                    try:
                                        review['image'] = await imgs_div[0].get_attribute('data-deferred-image-src')
                                    except:
                                        review['image'] = None
                                    try:
                                        review['name'] = await names_div[0].inner_text()
                                    except:
                                        review['name'] = None

                                    rate_div = await rates_div[0].query_selector_all('div')
                                    rate_txt = await rate_div[0].get_attribute('class')
                                    review['rate'] = float(re.sub('[^0-9]', '', rate_txt[len(rate_txt) - 8:])) // 2

                                    if len(posts_div) == 2:
                                        post0 = await posts_div[0].inner_text()
                                        post1 = await posts_div[1].inner_text()
                                        ch_post = post0 + "\n" + post1
                                        review['post'] = ch_post
                                    elif len(posts_div) == 1:
                                        review['post'] = await posts_div[0].inner_text()
                                    else:
                                        review['post'] = None

                                    review['date'] = await dates_div[0].inner_text()
                                    review['title'] = await titles_div[0].inner_text()
                                    expr_div = await exprs_div[0].query_selector_all('div')
                                    expr_p = await expr_div[0].query_selector_all(Selectors.g2_expr_p)
                                    for i in range(len(expr_p)):
                                        expr_ch = expr_ch + await expr_p[i].inner_text()
                                    review['experience'] = expr_ch
                                    review["provider"] = provider
                                    reviews.append(review)
                                    entities['reviews'] = reviews
                                    data_dict[job_id] = entities
                                    expr_ch = ""
                                if len(reviews) >= 20:
                                    break
                        except Exception as exception:
                            logger.info(exception)
                            self.status = "An Error Occurred During Getting Reviews"
                            self.status_dict['status'] = self.status
                            data_dict[job_id] = self.status_dict
                            return self.status
                        else:
                            logger.info("SCRAPING DONE.")
                            logger.info("REVIEWS SCRAPED = %s", len(reviews))
                            entities['reviews'] = reviews
            finally:
                await context.close()
                logger.info("CONTEXT CLOSED")
                await browser.close()
                logger.info("BROWSER CLOSED")
            data_dict[job_id] = entities
            return data_dict

    async def get_amazon_reviews(self, link, data_dict, job_id):
        async with async_playwright() as p:
            try:
                browser = await p.chromium.launch(headless=True)
                logger.info("BROWSER LAUNCHED..")
                context = await browser.new_context(locale='en-GB', bypass_csp=True, user_agent=self.useragent)

                provider = 'Amazon'
                created_at = datetime.datetime.now()
                expiration = created_at + timedelta.Timedelta(hours=self.hrs)
                expiration = expiration.isoformat()
                updated_at = created_at.isoformat()
                created_at = created_at.isoformat()
                new_uuid = job_id
                reviews = []

                page = await context.new_page()
                logger.info("PAGE CREATED..")

                page.set_default_navigation_timeout(timeout=0)
                await page.goto(link, timeout=0)
                logger.info("NAVIGATED TO LINK.. %s", link)
            except Exception as exception:
                logger.info(exception)
                self.status = "Browser Couldn't Start"
                self.status_dict['status'] = self.status
                data_dict[job_id] = self.status_dict
                return self.status
            else:
                entities = {'provider': provider, 'keyword': link, 'link': page.url, 'uuid': new_uuid,
                            'expiration': str(expiration), 'created_at': str(created_at), 'updated_at': str(updated_at)}
                data_dict[job_id] = entities

                logger.info("CHECKING REVIEWS AVAILABILITY..")
                check = await page.query_selector(Selectors.amazon_check)
                logger.info(check)
                if check is None:
                    self.status = "No Reviews Found"
                    self.status_dict['status'] = self.status
                    data_dict[job_id] = self.status_dict
                    return self.status
                try:
                    logger.info("REDIRECTING TO REVIEWS PAGE..")
                    review_page_btn = await page.query_selector(Selectors.amazon_review_page_btn)
                    await review_page_btn.click()
                    await asyncio.sleep(3)
                except Exception as exception:
                    logger.info(exception)
                    self.status = "Error Redirecting to Next Page"
                    self.status_dict['status'] = self.status
                    data_dict[job_id] = self.status_dict
                    return self.status
                else:
                    try:
                        logger.info("GETTING GENERAL STATS..")
                        stats_div = await page.query_selector(Selectors.amazon_stats_div)
                        gen_rate_div = await stats_div.query_selector(Selectors.amazon_gen_rate_dv)
                        gen_rate_txt = await gen_rate_div.inner_text()
                        index = gen_rate_txt.find("out")
                        gen_rating = gen_rate_txt[:index - 1]
                        tot_rev_div = await stats_div.query_selector(Selectors.amazon_tot_rev_div)
                        tot_rev_txt = await tot_rev_div.inner_text()
                        total_rev = re.sub('[^0-9]', '', tot_rev_txt)

                        entities['general rating'] = float(gen_rating)
                        entities['total reviews'] = int(total_rev)
                    except Exception as exception:
                        logger.info(exception)
                        self.status = "Error Getting General Stats"
                        self.status_dict['status'] = self.status
                        data_dict[job_id] = self.status_dict
                        return self.status
                    else:
                        try:
                            logger.info("SCRAPING REVIEWS..")
                            while True:
                                outer_div = await page.query_selector(Selectors.amazon_outer_div)
                                rev_div = await outer_div.query_selector_all(Selectors.amazon_rev_div)
                                for i in range(len(rev_div)):
                                    imgs_div = await rev_div[i].query_selector_all(Selectors.amazon_imgs_div)
                                    names_div = await rev_div[i].query_selector_all(Selectors.amazon_names_div)
                                    rates_div = await rev_div[i].query_selector_all(Selectors.amazon_rates_div)
                                    titles_div = await rev_div[i].query_selector_all(Selectors.amazon_titles_div)
                                    dates_div = await rev_div[i].query_selector_all(Selectors.amazon_dates_div)
                                    exprs_div = await rev_div[i].query_selector_all(Selectors.amazon_exprs_div)
                                    source_div = await rev_div[i].query_selector_all(Selectors.amazon_source_div)
                                    review = {'image': await imgs_div[0].get_attribute('src'),
                                              'name': await names_div[0].inner_text()}
                                    rate_txt = await rates_div[0].inner_text()
                                    review['rate'] = float(rate_txt[:3])
                                    date_txt = await dates_div[0].inner_text()
                                    index = date_txt.find("on")
                                    review['date'] = date_txt[index + 3:]
                                    try:
                                        review['title'] = await titles_div[2].inner_text()
                                    except:
                                        titles_form_div = await rev_div[i].query_selector(
                                            Selectors.amazon_titles_form_div)
                                        review['title'] = await titles_form_div.inner_text()
                                    try:
                                        review['experience'] = await exprs_div[0].inner_text()
                                    except:
                                        review['experience'] = None
                                    try:
                                        review['source'] = "https://www.amazon.com" + await source_div[
                                            0].get_attribute('href')
                                    except:
                                        review['source'] = None
                                    review["provider"] = provider
                                    reviews.append(review)
                                    entities['reviews'] = reviews
                                    data_dict[job_id] = entities
                                try:
                                    next_btn = await page.query_selector(Selectors.amazon_next_btn)
                                    cls = await next_btn.get_attribute("class")
                                    if cls == "a-disabled a-last":
                                        next_btn = None
                                    await next_btn.click()
                                    await asyncio.sleep(5)
                                    logger.info("REDIRECTING TO NEXT PAGE..")
                                    logger.info("SCRAPING REVIEWS OF NEXT PAGE..")
                                except:
                                    next_btn = None
                                if len(reviews) >= 100 or next_btn is None:
                                    break
                        except Exception as exception:
                            logger.info(exception)
                            self.status = "An Error Occurred During Getting Reviews"
                            self.status_dict['status'] = self.status
                            data_dict[job_id] = self.status_dict
                            return self.status
                        else:
                            logger.info("SCRAPING DONE.")
                            logger.info("REVIEWS SCRAPED = %s", len(reviews))
                            entities['reviews'] = reviews
            finally:
                await context.close()
                logger.info("CONTEXT CLOSED")
                await browser.close()
                logger.info("BROWSER CLOSED")
            data_dict[job_id] = entities
            return data_dict

    async def get_facebook_reviews(self, link, data_dict, job_id):
        async with async_playwright() as p:
            try:
                browser = await p.chromium.launch(headless=True)
                logger.info("BROWSER LAUNCHED..")
                context = await browser.new_context(locale='en-GB', bypass_csp=True)

                provider = 'Facebook'
                created_at = datetime.datetime.now()
                expiration = created_at + timedelta.Timedelta(hours=self.hrs)
                expiration = expiration.isoformat()
                updated_at = created_at.isoformat()
                created_at = created_at.isoformat()
                new_uuid = job_id
                reviews = []
                expr_ch = ""
                short_expr = False
                crushed_scroll = False

                page = await context.new_page()
                logger.info("PAGE CREATED..")

                page.set_default_navigation_timeout(timeout=0)
                await page.goto(link, timeout=0)
                logger.info("NAVIGATED TO LINK.. %s", link)
                await asyncio.sleep(2)
            except Exception as exception:
                logger.info(exception)
                self.status = "Browser Couldn't Start"
                self.status_dict['status'] = self.status
                data_dict[job_id] = self.status_dict
                return self.status
            else:

                entities = {'provider': provider, 'keyword': link, 'link': page.url, 'uuid': new_uuid,
                            'expiration': str(expiration), 'created_at': str(created_at), 'updated_at': str(updated_at)}
                data_dict[job_id] = entities

                await page.wait_for_selector(Selectors.facebook_rev_btn_div)
                await asyncio.sleep(3)
                try:
                    logger.info("LOADING REVIEWS..")
                    rev_btn_div = await page.query_selector(Selectors.facebook_rev_btn_div)
                    rev_btns = await rev_btn_div.query_selector_all(Selectors.facebook_rev_btns)
                    for t in range(len(rev_btns)):
                        if await rev_btns[t].inner_text() == "Reviews":
                            await rev_btns[t].click()
                            await asyncio.sleep(3)
                except Exception as exception:
                    logger.info(exception)
                    self.status = "Error Loading Reviews"
                    self.status_dict['status'] = self.status
                    data_dict[job_id] = self.status_dict
                    return self.status
                else:
                    await asyncio.sleep(2)
                    logger.info("CHECKING REVIEWS AVAILABILITY..")
                    check = await page.query_selector(Selectors.facebook_check)
                    if check is not None:
                        self.status = "No Reviews Found"
                        self.status_dict['status'] = self.status
                        data_dict[job_id] = self.status_dict
                        return self.status
                    else:
                        try:
                            logger.info("GETTING GENERAL STATS..")
                            stats = await page.query_selector(Selectors.facebook_stats)
                            stats_txt = await stats.inner_text()

                            logger.info("STATS => %s", stats_txt)
                            rate_ed_index = stats_txt.find("(")
                            gen_rating = stats_txt[8:rate_ed_index - 1]
                            total_rev = re.sub('[^0-9]', '', stats_txt[rate_ed_index:])

                            if rate_ed_index == -1:
                                stats = await page.query_selector(
                                    "span:has-text('Rating')")  # Selectors.facebook_stats
                                stats_txt = await stats.inner_text()

                                logger.info("STATS 2 => %s", stats_txt)
                                rate_ed_index = stats_txt.find("(")
                                gen_rating = stats_txt[8:rate_ed_index - 1]
                                total_rev = re.sub('[^0-9]', '', stats_txt[rate_ed_index:])

                            try:
                                entities['general rating'] = float(gen_rating)
                            except ValueError:
                                entities['general rating'] = None

                            try:
                                entities['total reviews'] = int(total_rev)
                            except ValueError:
                                entities['total reviews'] = None

                        except Exception as exception:
                            logger.info(exception)
                            self.status = "Error Getting General Stats"
                            self.status_dict['status'] = self.status
                            data_dict[job_id] = self.status_dict
                            return self.status
                        else:
                            try:
                                logger.info("PERFORMING SCROLL..")
                                start_time = time.time()
                                rev_len = 0
                                while True:
                                    full_rev_div = await page.query_selector(Selectors.facebook_full_rev_div)
                                    rev_div = await full_rev_div.query_selector_all(Selectors.facebook_rev_div)
                                    await page.mouse.wheel(0, 10000)
                                    await asyncio.sleep(3)
                                    if rev_len == len(rev_div):
                                        crushed_scroll = True
                                    rev_len = len(rev_div)
                                    current_time = time.time()
                                    timeout = current_time - start_time >= 60
                                    if len(rev_div) >= 100 or len(rev_div) >= int(
                                            total_rev) or timeout or crushed_scroll:
                                        break
                            except Exception as exception:
                                logger.info(exception)
                                self.status = "Error Performing Scroll"
                                self.status_dict['status'] = self.status
                                data_dict[job_id] = self.status_dict
                                return self.status
                            else:
                                try:
                                    logger.info("GETTING DATES...")
                                    date_list = []
                                    await asyncio.sleep(20)
                                    divs = await page.query_selector_all(Selectors.facebook_divs)
                                    for v in range(len(divs)):
                                        divs_html = await divs[v].inner_html()
                                        if divs_html != "":
                                            date_div = await divs[v].query_selector_all(Selectors.facebook_date_div)
                                    for j in range(len(date_div)):
                                        date_span = await date_div[j].query_selector("span")
                                        date_txt = await date_span.inner_text()
                                        if date_txt != "Learn More":
                                            date_list.append(date_txt)

                                    logger.info("SCRAPING REVIEWS..")
                                    await page.keyboard.press("Home")
                                    for i in range(len(rev_div)):
                                        img_div = await rev_div[i].query_selector(Selectors.facebook_img_div)
                                        name_div = await rev_div[i].query_selector(Selectors.facebook_name_div)
                                        expr_div = await rev_div[i].query_selector(Selectors.facebook_expr_div)
                                        source_div = await rev_div[i].query_selector(Selectors.facebook_source_div)
                                        await source_div.hover()

                                        if expr_div is None:
                                            short_expr_div = await rev_div[i].query_selector(
                                                Selectors.facebook_short_expr_div)
                                            short_expr = True
                                        else:
                                            try:
                                                see_more = await expr_div.query_selector(
                                                    Selectors.facebook_see_more)
                                                if see_more is not None:
                                                    await see_more.click(timeout=500)
                                            except:
                                                pass
                                        review = {'image': await img_div.get_attribute("xlink:href"),
                                                  'name': await name_div.inner_text(), 'date': date_list[i]}
                                        try:
                                            if not short_expr:
                                                expr_in_div = await expr_div.query_selector_all(
                                                    Selectors.facebook_expr_in_div)
                                                for j in range(len(expr_in_div)):
                                                    expr_txt = await expr_in_div[j].inner_text()
                                                    expr_ch = expr_ch + expr_txt + "\n"
                                                review['experience'] = expr_ch.strip()
                                                expr_ch = ""
                                            else:
                                                review['experience'] = await short_expr_div.inner_text()
                                                short_expr = False
                                        except Exception:
                                            review['experience'] = None
                                        try:
                                            review['source'] = await source_div.get_attribute('href')
                                        except Exception:
                                            review['source'] = None

                                        review["provider"] = provider
                                        reviews.append(review)
                                        entities['reviews'] = reviews
                                        data_dict[job_id] = entities
                                except Exception as exception:
                                    logger.info(exception)
                                    self.status = "An Error Occurred During Getting Reviews"
                                    self.status_dict['status'] = self.status
                                    data_dict[job_id] = self.status_dict
                                    return self.status
                                else:
                                    logger.info("SCRAPING DONE.")
                                    logger.info("REVIEWS SCRAPED = %s", len(reviews))
                                    entities['reviews'] = reviews
            finally:
                await context.close()
                logger.info("CONTEXT CLOSED")
                await browser.close()
                logger.info("BROWSER CLOSED")
            data_dict[job_id] = entities
            return data_dict

    async def get_trustpilot_reviews(self, link, data_dict, job_id):
        async with async_playwright() as p:
            try:
                browser = await p.chromium.launch(headless=True)
                logger.info("BROWSER LAUNCHED..")
                context = await browser.new_context(locale='en-GB', bypass_csp=True)

                provider = 'TrustPilot'
                created_at = datetime.datetime.now()
                expiration = created_at + timedelta.Timedelta(hours=self.hrs)
                expiration = expiration.isoformat()
                updated_at = created_at.isoformat()
                created_at = created_at.isoformat()
                new_uuid = job_id
                reviews = []

                page = await context.new_page()
                logger.info("PAGE CREATED..")

                page.set_default_navigation_timeout(timeout=0)
                await page.goto(link, timeout=0)
                logger.info("NAVIGATED TO LINK.. %s", link)
                await asyncio.sleep(2)
            except Exception as exception:
                logger.info(exception)
                self.status = "Browser Couldn't Start"
                self.status_dict['status'] = self.status
                data_dict[job_id] = self.status_dict
                return self.status
            else:
                entities = {'provider': provider, 'keyword': link, 'link': page.url, 'uuid': new_uuid,
                            'expiration': str(expiration), 'created_at': str(created_at), 'updated_at': str(updated_at)}
                data_dict[job_id] = entities

                logger.info("CHECKING REVIEWS AVAILABILITY..")
                check = await page.query_selector(Selectors.trust_pilot_check)
                if check is not None:
                    self.status = "No Reviews Found"
                    self.status_dict['status'] = self.status
                    data_dict[job_id] = self.status_dict
                    return self.status
                else:
                    try:
                        logger.info("HANDLING COOKIES..")
                        cookies_btn = await page.query_selector(Selectors.trust_pilot_cookies_btn)
                        await cookies_btn.click()
                    except Exception:
                        pass
                    else:
                        try:
                            logger.info("GETTING GENERAL STATS..")
                            stats = await page.query_selector(Selectors.trust_pilot_stats)
                            gen_rate_div = await stats.query_selector(Selectors.trust_pilot_gen_rate_div)
                            gen_rating = await gen_rate_div.inner_text()
                            tot_rev_div = await stats.query_selector(Selectors.trust_pilot_tot_rev_div)
                            tot_rev_txt = await tot_rev_div.inner_text()
                            total_rev = re.sub('[^0-9]', '', tot_rev_txt)
                            gen_rating = gen_rating.replace(',', '.')
                            total_rev = total_rev.replace(',', '.')
                            entities['general rating'] = float(gen_rating)
                            entities['total reviews'] = int(total_rev)
                        except Exception as exception:
                            logger.info(exception)
                            self.status = "Error Getting General Stats"
                            self.status_dict['status'] = self.status
                            data_dict[job_id] = self.status_dict
                            return self.status
                        else:
                            try:
                                logger.info("SETTING LANGUAGE..")
                                filter_btn = await page.query_selector(Selectors.trust_pilot_filter_btn)
                                await filter_btn.click()
                                popup = await page.query_selector(Selectors.trust_pilot_popup)
                                lang_radio = await popup.query_selector(Selectors.trust_pilot_lang_radio)
                                await lang_radio.click()
                                show_rev_btn = await popup.query_selector(Selectors.trust_pilot_show_rev_btn)
                                await show_rev_btn.click()
                                await asyncio.sleep(3)
                            except Exception:
                                pass
                            else:
                                try:
                                    logger.info("SCRAPING REVIEWS..")
                                    await page.keyboard.press("End")
                                    while True:
                                        rev_div = await page.query_selector_all(Selectors.trust_pilot_rev_div)
                                        for i in range(len(rev_div)):
                                            imgs_div = await rev_div[i].query_selector_all(
                                                Selectors.trust_pilot_imgs_div)
                                            name_div = await rev_div[i].query_selector(
                                                Selectors.trust_pilot_name_div)
                                            rate_div = await rev_div[i].query_selector(
                                                Selectors.trust_pilot_rate_div)
                                            date_div = await rev_div[i].query_selector(
                                                Selectors.trust_pilot_date_div)
                                            title_div = await rev_div[i].query_selector(
                                                Selectors.trust_pilot_title_div)
                                            expr_div = await rev_div[i].query_selector(
                                                Selectors.trust_pilot_expr_div)
                                            source_div = await rev_div[i].query_selector(
                                                Selectors.trust_pilot_source_div)
                                            review = {}
                                            try:
                                                review['image'] = await imgs_div[1].get_attribute('src')
                                            except:
                                                review['image'] = None
                                            review['name'] = await name_div.inner_text()
                                            rate_txt = await rate_div.get_attribute('alt')
                                            rate_index = rate_txt.find("out")
                                            rate = re.sub("[^0-9]", "", rate_txt[:rate_index - 1])
                                            review['rate'] = float(rate)
                                            review['date'] = await date_div.get_attribute('datetime')
                                            review['title'] = await title_div.inner_text()
                                            try:
                                                review['experience'] = await expr_div.inner_text()
                                            except:
                                                review['experience'] = None
                                            try:
                                                review[
                                                    'source'] = "https://www.trustpilot.com" + await source_div.get_attribute(
                                                    'href')
                                            except:
                                                review['source'] = None
                                            review["provider"] = provider
                                            reviews.append(review)
                                            entities['reviews'] = reviews
                                            data_dict[job_id] = entities
                                        try:
                                            nav = await page.query_selector(Selectors.trust_pilot_nav)
                                            next_btn = await nav.query_selector(Selectors.trust_pilot_next_btn)
                                            await next_btn.click()
                                            await asyncio.sleep(3)
                                            logger.info("REDIRECTING TO NEXT PAGE..")
                                            logger.info("SCRAPING REVIEWS OF NEXT PAGE..")
                                        except:
                                            next_btn = None
                                        if len(reviews) >= 100 or next_btn is None:
                                            break
                                except Exception as exception:
                                    logger.info(exception)
                                    self.status = "An Error Occurred During Getting Reviews"
                                    self.status_dict['status'] = self.status
                                    data_dict[job_id] = self.status_dict
                                    return self.status
                                else:
                                    logger.info("SCRAPING DONE.")
                                    logger.info("REVIEWS SCRAPED = %s", len(reviews))
                                    entities['reviews'] = reviews
            finally:
                await context.close()
                logger.info("CONTEXT CLOSED")
                await browser.close()
                logger.info("BROWSER CLOSED")
            data_dict[job_id] = entities
            return data_dict

    async def get_yelp_reviews(self, link, data_dict, job_id):
        async with async_playwright() as p:
            try:
                browser = await p.chromium.launch(headless=True)
                logger.info("BROWSER LAUNCHED..")
                context = await browser.new_context(locale='en-GB', bypass_csp=True)

                provider = 'Yelp'
                created_at = datetime.datetime.now()
                expiration = created_at + timedelta.Timedelta(hours=self.hrs)
                expiration = expiration.isoformat()
                updated_at = created_at.isoformat()
                created_at = created_at.isoformat()
                new_uuid = job_id
                reviews = []

                page = await context.new_page()
                logger.info("PAGE CREATED..")

                page.set_default_navigation_timeout(timeout=0)
                await page.goto(link, timeout=0)
                logger.info("NAVIGATED TO LINK.. %s", link)
                await asyncio.sleep(2)
            except Exception as exception:
                logger.info(exception)
                self.status = "Browser Couldn't Start"
                self.status_dict['status'] = self.status
                data_dict[job_id] = self.status_dict
                return self.status
            else:
                entities = {'provider': provider, 'keyword': link, 'link': page.url, 'uuid': new_uuid,
                            'expiration': str(expiration), 'created_at': str(created_at), 'updated_at': str(updated_at)}
                data_dict[job_id] = entities

                logger.info("CHECKING REVIEWS AVAILABILITY..")
                check = await page.query_selector(Selectors.yelp_check)
                if check is None:
                    self.status = "No Reviews Found"
                    self.status_dict['status'] = self.status
                    data_dict[job_id] = self.status_dict
                    return self.status
                else:
                    try:
                        logger.info("GETTING GENERAL STATS..")
                        stats_div = await page.query_selector(Selectors.yelp_stats_div)
                        gen_rate_div = await stats_div.query_selector(Selectors.yelp_gen_rate_div)
                        gen_rating = await gen_rate_div.inner_text()
                        tot_rev_div = await stats_div.query_selector(Selectors.yelp_tot_rev_div)
                        tot_rev_txt = await tot_rev_div.inner_text()
                        total_rev = re.sub('[^0-9]', '', tot_rev_txt)
                        entities['general rating'] = float(gen_rating)
                        entities['total reviews'] = int(total_rev)
                    except Exception as exception:
                        logger.info(exception)
                        self.status = "Error Getting General Stats"
                        self.status_dict['status'] = self.status
                        data_dict[job_id] = self.status_dict
                        return self.status
                    else:
                        try:
                            logger.info("SCRAPING REVIEWS..")
                            await page.keyboard.press("End")
                            while True:
                                rev_div = await page.query_selector_all(Selectors.yelp_rev_div)
                                for i in range(len(rev_div)):
                                    try:
                                        img_div = await rev_div[i].query_selector(Selectors.yelp_img_div)
                                        name_div = await rev_div[i].query_selector(Selectors.yelp_name_div)
                                        rate_div = await rev_div[i].query_selector(Selectors.yelp_rate_div)
                                        date_div = await rev_div[i].query_selector(Selectors.yelp_date_div)
                                        expr_div = await rev_div[i].query_selector(Selectors.yelp_expr_div)
                                        review = {}
                                        try:
                                            review['image'] = await img_div.get_attribute('src')
                                        except:
                                            review['image'] = None
                                        review['name'] = await name_div.inner_text()
                                        rate_txt = await rate_div.get_attribute('aria-label')
                                        rate_index = rate_txt.find('star')
                                        review['rate'] = float(rate_txt[:rate_index])
                                        review['date'] = await date_div.inner_text()
                                        try:
                                            review['experience'] = await expr_div.inner_text()
                                        except:
                                            review['experience'] = None
                                        review["provider"] = provider
                                        reviews.append(review)
                                        entities['reviews'] = reviews
                                        data_dict[job_id] = entities
                                    except:
                                        continue
                                try:
                                    next_btn = await page.query_selector(Selectors.yelp_next_btn)
                                    await next_btn.click()
                                    await asyncio.sleep(3)
                                    logger.info("REDIRECTING TO NEXT PAGE..")
                                    logger.info("SCRAPING REVIEWS OF NEXT PAGE..")
                                except:
                                    next_btn = None
                                if len(reviews) >= 100 or next_btn is None:
                                    break
                        except Exception as exception:
                            logger.info(exception)
                            self.status = "An Error Occurred During Getting Reviews"
                            self.status_dict['status'] = self.status
                            data_dict[job_id] = self.status_dict
                            return self.status
                        else:
                            logger.info("SCRAPING DONE.")
                            logger.info("REVIEWS SCRAPED = %s", len(reviews))
                            entities['reviews'] = reviews
            finally:
                await context.close()
                logger.info("CONTEXT CLOSED")
                await browser.close()
                logger.info("BROWSER CLOSED")
            data_dict[job_id] = entities
            return data_dict

    async def get_booking_reviews(self, link, data_dict, job_id):
        async with async_playwright() as p:
            try:
                browser = await p.chromium.launch(headless=True)
                logger.info("BROWSER LAUNCHED..")
                context = await browser.new_context(locale='en-GB', bypass_csp=True, user_agent=self.useragent)

                provider = 'Booking'
                created_at = datetime.datetime.now()
                expiration = created_at + timedelta.Timedelta(hours=self.hrs)
                expiration = expiration.isoformat()
                updated_at = created_at.isoformat()
                created_at = created_at.isoformat()
                new_uuid = job_id
                reviews = []

                page = await context.new_page()
                logger.info("PAGE CREATED..")

                page.set_default_navigation_timeout(timeout=0)
                await page.goto(link, timeout=0)
                logger.info("NAVIGATED TO LINK.. %s", link)
                await asyncio.sleep(5)
            except Exception as exception:
                logger.info(exception)
                self.status = "Browser Couldn't Start"
                self.status_dict['status'] = self.status
                data_dict[job_id] = self.status_dict
                return self.status
            else:
                entities = {'provider': provider, 'keyword': link, 'link': page.url, 'uuid': new_uuid,
                            'expiration': str(expiration), 'created_at': str(created_at), 'updated_at': str(updated_at)}
                data_dict[job_id] = entities

                logger.info("CHECKING REVIEWS AVAILABILITY..")
                await page.mouse.wheel(0, 3000)
                await asyncio.sleep(2)
                check = await page.query_selector(Selectors.booking_check)
                if check is None:
                    self.status = "No Reviews Found"
                    self.status_dict['status'] = self.status
                    data_dict[job_id] = self.status_dict
                    return self.status
                else:
                    try:
                        logger.info("GETTING GENERAL STATS..")
                        stats_div = await page.query_selector(Selectors.booking_stats_div)
                        gen_rate_div = await stats_div.query_selector(Selectors.booking_gen_rate_div)
                        gen_rating = await gen_rate_div.inner_text()
                        tot_rev_div = await stats_div.query_selector(Selectors.booking_tot_rev_div)
                        tot_rev_txt = await tot_rev_div.inner_text()
                        total_rev = re.sub('[^0-9]', '', tot_rev_txt)
                        entities['general rating'] = float(gen_rating)
                        entities['total reviews'] = int(total_rev)
                    except Exception as exception:
                        logger.info(exception)
                        self.status = "Error Getting General Stats"
                        self.status_dict['status'] = self.status
                        data_dict[job_id] = self.status_dict
                        return self.status
                    else:
                        try:
                            logger.info("LOADING REVIEWS..")
                            read_all_rev = await page.query_selector(Selectors.booking_read_all_rev)
                            await read_all_rev.click()
                            await asyncio.sleep(10)
                        except Exception as exception:
                            logger.info(exception)
                            self.status = "Error Loading Reviews"
                            self.status_dict['status'] = self.status
                            data_dict[job_id] = self.status_dict
                            return self.status
                        else:
                            try:
                                logger.info("SCRAPING REVIEWS..")
                                popup = await page.query_selector(Selectors.booking_popup)
                                while True:
                                    rev_div = await popup.query_selector_all(Selectors.booking_rev_div)
                                    for i in range(len(rev_div)):
                                        img_div = await rev_div[i].query_selector(Selectors.booking_img_div)
                                        name_div = await rev_div[i].query_selector(Selectors.booking_name_div)
                                        rate_div = await rev_div[i].query_selector(Selectors.booking_rate_div)
                                        date_outer_div = await rev_div[i].query_selector(
                                            Selectors.booking_date_outer_div)
                                        title_div = await rev_div[i].query_selector(Selectors.booking_title_div)
                                        expr_div = await rev_div[i].query_selector_all(Selectors.booking_expr_div)
                                        review = {}
                                        try:
                                            review['image'] = await img_div.get_attribute('src')
                                        except:
                                            review['image'] = None
                                        review['name'] = await name_div.inner_text()
                                        review['rate'] = float(await rate_div.inner_text()) // 2
                                        date_div = await date_outer_div.query_selector(Selectors.booking_date_div)
                                        date_txt = await date_div.inner_text()
                                        date_index = date_txt.find(":")
                                        review['date'] = date_txt[date_index + 1:]
                                        review['title'] = await title_div.inner_text()
                                        try:
                                            for x in range(len(expr_div)):
                                                icon_div = await expr_div[x].query_selector("svg")
                                                icon_type = await icon_div.get_attribute("class")
                                                icon_positive_index = icon_type.find("great")
                                                icon_negative_index = icon_type.find("poor")
                                                if icon_positive_index != -1:
                                                    expr_txt = await expr_div[x].query_selector(
                                                        Selectors.booking_expr_txt)
                                                    review['experience'] = await expr_txt.inner_text()
                                                elif icon_negative_index != -1:
                                                    expr_txt = await expr_div[x].query_selector(
                                                        Selectors.booking_expr_txt)
                                                    review['poor experience'] = await expr_txt.inner_text()
                                        except:
                                            review['experience'] = None
                                        review["provider"] = provider
                                        reviews.append(review)
                                        entities['reviews'] = reviews
                                        data_dict[job_id] = entities
                                    try:
                                        next_btn = await popup.query_selector(Selectors.booking_next_btn)
                                        await next_btn.click()
                                        await asyncio.sleep(3)
                                        logger.info("REDIRECTING TO NEXT PAGE..")
                                        logger.info("SCRAPING REVIEWS OF NEXT PAGE..")
                                    except:
                                        next_btn = None
                                    if len(reviews) >= 100 or next_btn is None:
                                        break
                            except Exception as exception:
                                logger.info(exception)
                                self.status = "An Error Occurred During Getting Reviews"
                                self.status_dict['status'] = self.status
                                data_dict[job_id] = self.status_dict
                                return self.status
                            else:
                                logger.info("SCRAPING DONE.")
                                logger.info("REVIEWS SCRAPED = %s", len(reviews))
                                entities['reviews'] = reviews
            finally:
                await context.close()
                logger.info("CONTEXT CLOSED")
                await browser.close()
                logger.info("BROWSER CLOSED")
            data_dict[job_id] = entities
            return data_dict
