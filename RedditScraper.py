import requests, time, json, os, sys
import praw, pprint, prawcore
import datetime as dt

url = "https://api.pushshift.io/reddit/search/submission"

def crawl_page(subreddit: str, last_page = None):
  """Crawl a page of results from a given subreddit.

  :param subreddit: The subreddit to crawl.
  :param last_page: The last downloaded page.

  :return: A page or results.
  """
  params = {"subreddit": subreddit, "size": 500, "sort": "desc", "sort_type": "created_utc"}
  if last_page is not None:
    if len(last_page) > 0:
      # resume from where we left at the last page
      params["before"] = last_page[-1]["created_utc"]
    else:
      # the last page was empty, we are past the last page
      return []
  # use try-except to catch all possible exceptions
  try:
    # specify timeout value to avoid indefinite waiting
    results = requests.get(url, params, timeout=120)
    if not results.ok:
      raise Exception("Server returned status code {}".format(results.status_code))
  except Exception as e:
    # something wrong happened
    raise e
  return results.json()["data"]


def get_comments_json(submission_batch):
  reddit = praw.Reddit(client_id='aHuYIBlO0Ys1CA', client_secret='PVumAMVFHa_WKu3YpOalgh0_axk',
                       user_agent='GPT-2 Comedian')
  data = []
  i = 1
  print("start collecting %d comments..." % len(submission_batch))
  for s in submission_batch:
    record = {'theme' : s['theme'], 'author': s['author'],
            'title' : s['title'], 'body' : s['body'],
            'created_utc' : s['created_utc'], 'comments':[]}
    try:
      submission = praw.models.Submission(reddit, s['id'])
      submission.comments.replace_more(limit=0)
      submission.comment_sort = "best"
      j = 0
      for comm in submission.comments:
        if comm.author is None:
          author = "[deleted]"
        else:
          author = comm.author.name
        if not comm.body == "[deleted]" and j < 5:
          j+=1
          record['comments'].append({'author':author,'body':comm.body})
        else:
          continue
    except prawcore.exceptions.NotFound as e:
      print(type(e))
      print(e)
      print("failed to collect %d comment..." % i)
    data.append(record)
    print("collecting comment %d..." % i)
    i+=1
    time.sleep(.025)
  print("finish collecting %d comments..." % len(submission_batch))
  return data


def crawl_subreddit(subreddit, submissions, subreddit_file_name, max_submissions = -1):
  """
  Crawl submissions from a subreddit.

  :param subreddit: The subreddit to crawl.
  :param submissions: The existing submissions loaded from subreddit_file_name.
  :param subreddit_file_name: The file to dump expanded submissions to.
  :param max_submissions: The maximum number of submissions to download.

  :return: A list of submissions, a bool flag to indicate if the crawl is finished.
  """

  # use try-except to handle mannual interruption (CTRL-C)
  try:
    is_finished = True
    last_page = [submissions[-1]] if submissions else None
    infinite = max_submissions <= 0
    while last_page != [] and (infinite or len(submissions) < max_submissions):
      # FIX: use try-except to make page crawl process survive network issues
      try:
        print("start collecting the %dth submission in time order..." % (len(submissions) + 1))
        temp_page = crawl_page(subreddit, last_page)
        new_subs = filter(lambda s: not 'media' in s or s['title'] == '[removed]',temp_page)
        submission_batch = list(map(lambda s: {
                            'theme' : subreddit,
                            'author':s['author'],
                            'title':s['title'],
                            'created_utc':s['created_utc'],
                            'body': '' if not 'selftext' in s else s['selftext'],
                            'id':s['id']}, new_subs))
        submission_batch = get_comments_json(submission_batch)
      except KeyboardInterrupt as e:
        raise e
      except Exception as e:
        print(e)
        print("failed to collect the %dth submission in time order..." % (len(submissions) + 1))
        time.sleep(.025)
        continue
      last_page = temp_page
      submissions += submission_batch
      print("finish collecting %d submissions..." % len(submissions))
      time.sleep(.025)
  except KeyboardInterrupt:
    print("Got external abortion, dump page data...")
    is_finished = False
  finally:
    submissions = submissions if infinite else submissions[:max_submissions]
    with open(subreddit_file_name, 'w') as fd:
      fd.write(json.dumps(submissions))
    return submissions, is_finished


# helper function to crawl pages under a specific subreddit
def collect_all_data_subreddit(subreddit_scraped):
  subreddit_file_name = "%s_submissions.json" % subreddit_scraped
  submissions = []
  if os.path.exists(subreddit_file_name) and os.path.getsize(subreddit_file_name) > 0:
    print("/****** start loading previous data for %s ******/" % subreddit_scraped)
    with open(subreddit_file_name, 'r') as fd:
      submissions = json.load(fd)
    print("/****** finish loading previous data for %s ******/" % subreddit_scraped)
  print("/****** start collecting data for %s ******/" % subreddit_scraped)
  astest_submissions, is_finished = crawl_subreddit(subreddit_scraped, submissions, subreddit_file_name, -1)
  print("total #submission is:", len(astest_submissions))
  # a simple content check for first and last submission
  if (len(astest_submissions) > 0):
    print("first submission is:", astest_submissions[0])
    print("last submission is:", astest_submissions[-1])
  print("/****** finish collecting data for %s ******/" % subreddit_scraped)
  return astest_submissions, is_finished


if __name__ == '__main__':
  astest_submissions, is_finished = collect_all_data_subreddit(sys.argv[1])
  if is_finished:
    print('/****** data is fully collected now ******/')
    print('/****** start to migrate data from json file to txt file ******/')
    for submission in astest_submissions:
      submission.pop('created_utc', None)
    with open("%s_submissions.txt" % sys.argv[1], 'w') as fd:
      fd.write(json.dumps(astest_submissions))
    print('/****** end migrating data from json file to txt file ******/')
  else:
    print('/****** data is not fully collected yet ******/')
