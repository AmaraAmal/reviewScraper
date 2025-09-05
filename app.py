import asyncio
from flask import Flask, request, jsonify
from CollectData import GetReviews
from ExpirationDictionary import DictionaryWithTimeout
import uuid
import concurrent.futures
import logging

num_threads = 4
expiration_time_seconds = 240

thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=num_threads)
tasks = DictionaryWithTimeout(timeout_seconds=expiration_time_seconds)
data_dict = DictionaryWithTimeout(timeout_seconds=expiration_time_seconds)


def start_background_task(keyword, provider, job_id):
    global target_task
    if provider == "Google":
        target_task = collector.get_google_reviews
    elif provider == "AirBnB":
        target_task = collector.get_airbnb_reviews
    elif provider == "Amazon":
        target_task = collector.get_amazon_reviews
    elif provider == "eBay":
        target_task = collector.get_ebay_reviews
    elif provider == "G2":
        target_task = collector.get_g2_reviews
    elif provider == "PlayStore":
        target_task = collector.get_play_store_reviews
    elif provider == "TripAdvisor":
        target_task = collector.get_trip_advisor_reviews
    elif provider == "Etsy":
        target_task = collector.get_etsy_reviews
    elif provider == "Facebook":
        target_task = collector.get_facebook_reviews
    elif provider == "Capterra":
        target_task = collector.get_capterra_reviews
    elif provider == "TrustPilot":
        target_task = collector.get_trustpilot_reviews
    elif provider == "Yelp":
        target_task = collector.get_yelp_reviews
    elif provider == "Booking":
        target_task = collector.get_booking_reviews

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(target_task(keyword, data_dict=data_dict, job_id=job_id))
    loop.close()


app = Flask(__name__)
logging.basicConfig(filename='app.log', level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("app")

collector = GetReviews()


@app.route("/reviews/collect", methods=['POST'])
async def get_data():
    request_data = request.get_json()
    keyword = request_data.get('keyword')
    provider = request_data.get('provider')
    matching_tasks = [(job_id, data[0]) for job_id, data in tasks.items() if
                      data[0]["provider"] == provider and data[0]["keyword"] == keyword]
    if not matching_tasks:
        if len(tasks)+1 > num_threads:
            return jsonify({'error': "server busy try again later"}), 202
        job_id = str(uuid.uuid4())
        future = thread_pool.submit(start_background_task, keyword, provider, job_id)
        tasks[job_id] = {'task': future, 'keyword': keyword, 'provider': provider}
        return jsonify({'job_id': job_id}), 202
    else:
        for job_id, data in matching_tasks:
            return jsonify({'job_id': job_id}), 202


@app.route('/reviews/check', methods=['POST'])
async def check_task_status():
    request_data = request.get_json()
    job_id = request_data.get('job_id')
    terminate = request_data.get('terminate')
    if terminate is None:
        terminate = False

    if job_id not in tasks:
        return jsonify({"status": "Error", 'error': 'Job not found'}), 404

    task = tasks[job_id]
    if terminate:
        task['task'].cancel()
        try:
            await task['task']
        except:
            tasks.__delitem__(job_id)
            return jsonify({'status': 'Terminated Successfully'})

        return jsonify({'status': 'Failed to Terminate'})

    if not task['task'].done():
        if job_id in data_dict:
            return jsonify({'status': 'in_progress', 'result': data_dict[job_id]})
        return jsonify({'status': 'in_progress'})

    else:
        if job_id not in data_dict:
            tasks.__delitem__(job_id)
            return jsonify({"status": "Error", 'error': 'There was an error while scraping'}), 404
        result = data_dict[job_id]
        if result.get("status") is not None:
            tasks.__delitem__(job_id)
        return jsonify({'status': 'completed', 'result': result})


def run_server():
    from waitress import serve
    serve(app, host="0.0.0.0", port=5001)


if __name__ == "__main__":
    run_server()
