import pandas as pd
import statistics
from config import *
from find_toxicity import find_toxicity


def _find_new_users(repo, conv_type, since, users):
  new_users = []
  for user in users:
    user_conv = repo.get_issues(state="all", creator=user)
    numbers = [u.number for u in user_conv]
    numbers.sort()
    numbers.reverse()
    found = False
    ind = -1
    while not found:
      ind_res = repo.get_issue(number=numbers[ind])
      ind-=1
      if conv_type == "issue":
        if ind_res.pull_request is None:
          found = True
      elif conv_type == "pr":
        if ind_res.pull_request is not None:
          found = True
    if DEBUG:
      print(user, numbers[ind+1], conv_type, ind_res.created_at)
    if ind_res.created_at >= since:
      new_users.append(user)
  return new_users
  

def cal_metrics(repo, conv_type, all_dis, since, end):
  if len(all_dis) == 0:
    res = {
        "num_closed": 0,
        "num_closed_0_comments": 0,
        "avg_close_time": 0,
        "median_close_time": 0,
        "median_comments_recent": 0,
        "avg_comments_recent": 0,
        "num_open": 0,
        "num_unique_authors": 0,
        "unique_authors": [],
        "new_authors": 0,
        "num_new_users": 0,
        "avg_comments": 0,
        "median_commenst": 0,
        "new_label_counts": {},
        "num_toxic": 0,
        "toxic": [],
        "neg_senti": [],
        "max_toxic": 0,
        "max_attack": 0
    }
    if conv_type == "pr":
      res["total_merged"] = 0
    return res

  # closed in that month
  cur_win_closed = all_dis.loc[
          (all_dis["state"]=="closed") &
          (all_dis["closed_at"]>=since) &
          (all_dis["closed_at"]<end)]
  num_closed = len(cur_win_closed)
  
  if num_closed > 0:
    # calculate avg time before close
    median_close_time = statistics.median(cur_win_closed["close_len"])
    avg_close_time = statistics.mean(cur_win_closed["close_len"])
    # avg num comments before closing
    median_comments_before_close = statistics.median(cur_win_closed["num_comments"])
    avg_comments_before_close = statistics.mean(cur_win_closed["num_comments"])
  else:
    median_close_time = 0
    avg_close_time = 0
    median_comments_before_close = 0
    avg_comments_before_close = 0

  # open in that month
  cur_win_open = all_dis.loc[
          (all_dis["created_at"]>=since) &
          (all_dis["created_at"]<end)]
  num_open = len(cur_win_open)
  if num_open > 0:
    median_comments_recent = statistics.median(cur_win_open["num_comments"])
    avg_comments_recent = statistics.mean(cur_win_open["num_comments"])
  else:
    median_comments_recent = 0
    avg_comments_recent = 0

  # users who opened issue/pr in that month
  unique_authors = list(set(cur_win_open["author"].tolist()))
  num_unique_authors = len(unique_authors)

  new_users = _find_new_users(repo, conv_type, since, unique_authors)
  new_users = [n.login for n in new_users]
  num_new_users = len(new_users)

  unique_authors = [a.login for a in unique_authors]

  toxic_convs = find_toxicity(repo, cur_win_open, since, end)
  num_toxic = len(toxic_convs["toxic"]) + len(toxic_convs["neg_senti"])

  res = {
      "num_closed": num_closed,
      "num_closed_0_comments": len(
              cur_win_closed.loc[
                    cur_win_closed["num_comments"]==0]),
      "median_close_time": round(median_close_time, 1),
      "avg_close_time": round(avg_close_time, 1),
      "num_open": num_open,
      "num_unique_authors": num_unique_authors,
      "unique_authors": unique_authors,
      "new_authors": new_users,
      "num_new_users": num_new_users,
      "median_comments_before_close": round(median_comments_before_close, 1),
      "avg_comments_before_close": round(avg_comments_before_close, 1),
      "median_comments_recent": round(median_comments_recent, 1),
      "avg_comments_recent": round(avg_comments_recent, 1),
      "num_toxic": num_toxic,
      "toxic": toxic_convs["toxic"],
      "neg_senti": toxic_convs["neg_senti"],
      "max_toxic": round(toxic_convs["max_toxic"], 3),
      "max_attack": round(toxic_convs["max_attack"], 3)
  }
  if conv_type == "pr":
    res["total_merged"] = len(cur_win_closed.loc[cur_win_closed["merged_at"]!=None])
  return res
