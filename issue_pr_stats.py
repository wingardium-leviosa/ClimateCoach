import os
import json
import time
from github import Github
import requests
from datetime import datetime
import pandas as pd
import dateutil.relativedelta
from cal_metrics import cal_metrics
from config import *
from collections import defaultdict

def get_time(prev_month):
  d = datetime.today()
  since = d - dateutil.relativedelta.relativedelta(months=prev_month)
  if DEBUG:
    since = datetime.strptime("2022-05-26", "%Y-%m-%d")
  since = datetime.combine(since, datetime.min.time())
  if DEBUG:
    print(since)
  return since


# count how many issue/pr in each label
def _count_labels(all_dis, since, end):
  cur_win_open = all_dis.loc[
          (all_dis["created_at"]>=since) &
          (all_dis["created_at"]<end)]
  labels = cur_win_open["label"].tolist()
  labels = [f for l in labels for f in l]
  labels = list(set(labels))
  label_counts = {}
  for label in labels:
    contains = [label in dis_labels for dis_labels in all_dis["label"].tolist()]
    label_counts[label] = len(all_dis.iloc[contains])
  return label_counts


def _check_quota(g, repo):
  quota = g.get_rate_limit().core.remaining
  if quota < 10:
    TOKEN_ID = not TOKEN_ID
    g = Github(TOKENS[int(TOKEN_ID)])
    repo = g.get_repo(SLUG)
    if DEBUG:
      print("change token")
  return [g, repo]


def _mine_convers(g, repo):
  since = get_time(6)
  res = repo.get_issues(state="all", since=since)

  issues = []
  prs = []
  count = 0
  for ind in res:
    count += 1
    if count > 3000:
      g, repo = _check_quota(g, repo)
    num = ind.number
    ind_res = repo.get_issue(number=num)

    if ind_res.closed_at != None:
      close_len = (ind_res.closed_at - ind_res.created_at).days
      open_for = close_len
    else:
      close_len = -1
      open_for = (datetime.today() - ind_res.created_at).days

    cur_dict = {
            "number":num,
            "title": ind_res.title,
            "url": ind_res.html_url,
            "author": ind_res.user,
            "created_at":ind_res.created_at,
            "closed_at":ind_res.closed_at,
            "state":ind_res.state,
            "close_len": close_len,
            "open_for": open_for,
            "num_comments":ind_res.comments,
            "label":[ind.name for ind in ind_res.labels]
    }
    if ind_res.pull_request != None:
      cur_dict["merged_at"] = repo.get_pull(number=num).merged_at
      prs.append(cur_dict)
    else:
      issues.append(cur_dict)
      
  return [pd.DataFrame(issues), pd.DataFrame(prs)]
  

# get all issues in the past half a year
def get_convs(g):
  repo = g.get_repo(SLUG)
  [issues, prs] = _mine_convers(g, repo)
  if DEBUG:
    issues.to_csv("test_issues.csv", index=False)
    prs.to_csv("test_prs.csv", index=False)

  # get each prev month
  res = {
        "issues": defaultdict(list),
        "prs": defaultdict(list)
  }

  for i in range(END_MON, 0, -1): 
    [g, repo] = _check_quota(g, repo)
    since = get_time(i)
    end = get_time(i-1)

    if DEBUG:
      end = datetime.today()

    metric_dict_issues = cal_metrics(repo, "issue", issues, since, end)
    for key in metric_dict_issues.keys():
      res["issues"][key].append(metric_dict_issues[key])
    if i == 1:
      label_counts = _count_labels(issues, since, end)
      res["issues"]["label_counts_keys"] = list(label_counts.keys())
      res["issues"]["label_counts_values"] = list(label_counts.values())

    metric_dict_prs = cal_metrics(repo, "pr", prs, since, end)
    for key in metric_dict_prs.keys():
      res["prs"][key].append(metric_dict_prs[key])
    if i == 1:
      label_counts = _count_labels(prs, since, end)
      res["prs"]["label_counts_keys"] = list(label_counts.keys())
      res["prs"]["label_counts_values"] = list(label_counts.values())

    res["issues"]["unique_authors"] = res["issues"]["unique_authors"][-1]
    res["prs"]["unique_authors"] = res["prs"]["unique_authors"][-1]
    res["issues"]["new_authors"] = res["issues"]["new_authors"][-1]
    res["prs"]["new_authors"] = res["prs"]["new_authors"][-1]

    open_issues = issues.loc[issues["state"]=="open"]
    open_issues = open_issues.sort_values(
            by=["open_for"],
            ascending=False
    )
    res["issues"]["long_standing"] = [
            dict(x) for i, x in open_issues[["title", "url"]].iterrows()][:3]


    open_prs = prs.loc[prs["state"]=="open"]
    open_prs = open_prs.sort_values(
            by=["open_for"],
            ascending=False
    )
    res["prs"]["long_standing"] = [
            dict(x) for i, x in open_prs[["title", "url"]].iterrows()][:3]

    open_issues = open_issues.sort_values(
            by=["num_comments"],
            ascending=False
    )
    res["issues"]["most_comments"] = [
            dict(x) for i, x in open_issues[["title", "url"]].iterrows()][:3]

    open_prs = open_prs.sort_values(
            by=["num_comments"],
            ascending=False
    )
    res["prs"]["most_comments"] = [
            dict(x) for i, x in open_prs[["title", "url"]].iterrows()][:3]

    total_active = len(
          set(metric_dict_issues["unique_authors"]).union(
              set(metric_dict_prs["unique_authors"])))
    total_new = len(
          set(metric_dict_issues["new_authors"]).union(
              set(metric_dict_prs["new_authors"])))
    res["total"] = {
          "total_active": total_active,
          "total_new": total_new} 
  return(res)


g = Github(TOKENS[TOKEN_ID])
print(g.get_rate_limit().core.remaining)
stats = get_convs(g)
print(stats)
out = open("output.json", "w")
out.write("[")
out.write(json.dumps(stats["issues"]))
out.write(",")
out.write(json.dumps(stats["prs"]))
out.write(",")
out.write(json.dumps(stats["total"]))
out.write("]")
out.close()
