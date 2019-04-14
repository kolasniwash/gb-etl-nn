
import pandas as pd
import requests as rq
import json
import io
import datetime

def get_data():
	#query today's date. construct two dates. one today, one 30 days ago.
	today = datetime.date.today()
	last30 = today - datetime.timedelta(days=30)
	today = str(today)
	last30 = str(last30)

	#generate the event and revenue access token for the payload.
	tokens = ["udctlp", "9okrzo", "awwr8i", "bja0fi", "gsblgn", "pcw1dl", "ukw0nk", "g7rad1", "gvcwfs", "8m98yt", "w2teee", "nn5l8r", "sr4sdl", "3nxr3f", "buuugy", "pwqi9z", "gswco5", ","]

	events = "_events,"
	revenue = "_revenue,"

	event_tokens = events.join(tokens)
	event_tokens = event_tokens[:-2]

	revenue_tokens = revenue.join(tokens)
	revenue_tokens = revenue_tokens[:-2]

	all_tokens = [event_tokens, revenue_tokens]

	comma = ","
	events_tokens = comma.join(all_tokens)

	#Adjust access tokens and payload
	user_token = {"Authorization" : "Token 6kkwgMVq7wEgzXFwckTB"}
	app_token = "x38xxb8m41ds"
	kpis = "impressions,clicks,installs,limit_ad_tracking_installs,reattributions,sessions,cohort_revenue,revenue,daus,waus,maus,gdpr_forgets"

	payload = {"start_date" : last30, "end_date" : today,  "kpis" : kpis , "event_kpis" : events_tokens,
	          "grouping" : "day, networks, campaigns, adgroups, creatives, country, os_name",
	            }

	#get requests for sessions, clicks, installs
	response = rq.get("https://api.adjust.com/kpis/v1/" + app_token + ".csv", headers = user_token, params = payload)
	status = response.status_code
	print(status)

	#creates a datframe the get responces for sessions, clicks, installs
	data = pd.read_csv(io.StringIO(response.text))
	data_adjust = pd.DataFrame(data)

	#sets the coloum names from the defaul deliverables csv
	data_adjust.columns = ['date', 'tracker_token', 'network', 'campaign', 'adgroup', 'creative', 'country', 'os_name',
                       'impressions', 'clicks', 'installs','limit_ad_tracking_installs', 'reattributions', 'sessions',
                       'cohort_revenue', 'revenue', 'daus', 'waus', 'maus', 'gdpr_forgets',
                       "Age Group >= 3 (udctlp) (Events)" ,"Age provided (9okrzo) (Events)", "Assessment 1 done (awwr8i) (Events)", "Assessment 2 done (bja0fi) (Events)", "Assessment 3 done (gsblgn) (Events)", "Assessment 4 done (pcw1dl) (Events)", "Checkout Seen (ukw0nk) (Events)", "End Onboarding (g7rad1) (Events)", "Package Selected (gvcwfs) (Events)", "Purchase (8m98yt) (Events)", "Registration Done (w2teee) (Events)", "Session 1 Done (nn5l8r) (Events)", "Session 1 Started (sr4sdl) (Events)", "Start Onboarding (3nxr3f) (Events)", "Training Intensity selected (buuugy) (Events)", "Welcome Screen Seen (pwqi9z) (Events)", "s_story_open_read_more (gswco5) (Events)",
                       "Age Group >= 3 (udctlp) (Revenue)", "Age provided (9okrzo) (Revenue)","Assessment 1 done (awwr8i) (Revenue)","Assessment 2 done (bja0fi) (Revenue)","Assessment 3 done (gsblgn) (Revenue)","Assessment 4 done (pcw1dl) (Revenue)","Checkout Seen (ukw0nk) (Revenue)","End Onboarding (g7rad1) (Revenue)","Package Selected (gvcwfs) (Revenue)","Purchase (8m98yt) (Revenue)","Registration Done (w2teee) (Revenue)","Session 1 Done (nn5l8r) (Revenue)","Session 1 Started (sr4sdl) (Revenue)","Start Onboarding (3nxr3f) (Revenue)","Training Intensity selected (buuugy) (Revenue)","Welcome Screen Seen (pwqi9z) (Revenue)","s_story_open_read_more (gswco5) (Revenue)"
                      ]

	return data_adjust
