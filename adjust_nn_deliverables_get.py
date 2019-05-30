
import pandas as pd
import requests as rq
import json
import io
import datetime
import settings

def get_data(start, finish):

	'''
	A function that retreves the last 30 days worh of click data from adjust's api and formats it.
	Formatted data is returned in the form of a dataframe, with column headers appropriate for each metric.
	'''
#
#	#query today's date. construct two dates. one today, one 30 days ago.
#	yesterday = finish #datetime.date.today()
#	last30 = yesterday - datetime.timedelta(days=30)
#	yesterday = str(yesterday)
#	last30 = str(last30)

	#generate the event and revenue access token for the payload.
	tokens = ["udctlp", "9okrzo", "awwr8i", "bja0fi", "gsblgn", "pcw1dl", "ukw0nk", "g7rad1", "gvcwfs", "8m98yt", "w2teee", "nn5l8r", 
		"sr4sdl", "3nxr3f", "buuugy", "pwqi9z", "gswco5", ","]

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
	user_token = {"Authorization" : settings.API_KEY_cur}
	app_token = settings.token_key_cur
	kpis = "impressions,clicks,installs,limit_ad_tracking_installs,reattributions,sessions,cohort_revenue,revenue,daus,waus,maus,gdpr_forgets"

	payload = {"start_date" : start, "end_date" : finish,  "kpis" : kpis , "event_kpis" : events_tokens,
	          "grouping" : "day, networks, campaigns, adgroups, creatives, country, os_name",
	            }

	#get requests for sessions, clicks, installs
	response = rq.get("https://api.adjust.com/kpis/v1/" + app_token + ".csv", headers = user_token, params = payload)
	status = response.status_code
	print('Active data:')
	print(status)

	#creates a datframe the get responces for sessions, clicks, installs
	data = pd.read_csv(io.StringIO(response.text))
	data_adjust = pd.DataFrame(data)

	#sets the coloum names from the defaul deliverables csv
	data_adjust.columns = ['date', 'tracker_token', 'network', 'campaign', 'adgroup', 
                               'creative', 'country', 'os_name', 'impressions', 'clicks', 
                               'installs', 'limit_ad_tracking_installs', 'reattributions', 
                               'sessions', 'cohort_revenue', 'revenue', 'daus', 'waus', 
                               'maus', 'gdpr_forgets', 'age_group_greater_than_3_udctlp_events', 
                               'age_provided_9okrzo_events', 'assessment_1_done_awwr8i_events', 
                               'assessment_2_done_bja0fi_events', 'assessment_3_done_gsblgn_events', 
                               'assessment_4_done_pcw1dl_events', 'checkout_seen_ukw0nk_events', 
                               'end_onboarding_g7rad1_events', 'package_selected_gvcwfs_events', 
                               'purchase_8m98yt_events', 'registration_done_w2teee_events', 
                               'session_1_done_nn5l8r_events', 'session_1_started_sr4sdl_events', 
                               'start_onboarding_3nxr3f_events', 'training_intensity_selected_buuugy_events', 
                               'welcome_screen_seen_pwqi9z_events', 's_story_open_read_more_gswco5_events', 
                               'age_group_greater_than_3_udctlp_revenue', 'age_provided_9okrzo_revenue', 
                               'assessment_1_done_awwr8i_revenue', 'assessment_2_done_bja0fi_revenue', 
                               'assessment_3_done_gsblgn_revenue', 'assessment_4_done_pcw1dl_revenue', 
                               'checkout_seen_ukw0nk_revenue', 'end_onboarding_g7rad1_revenue', 
                               'package_selected_gvcwfs_revenue', 'purchase_8m98yt_revenue', 
                               'registration_done_w2teee_revenue', 'session_1_done_nn5l8r_revenue', 
                               'session_1_started_sr4sdl_revenue', 'start_onboarding_3nxr3f_revenue', 
                               'training_intensity_selected_buuugy_revenue', 'welcome_screen_seen_pwqi9z_revenue', 
                               's_story_open_read_more_gswco5_revenue']


	#Adjust access tokens and payload
	user_token_leg = {"Authorization" : settings.API_KEY_legacy}
	app_token_leg = settings.token_key_legacy
	kpis = "impressions,clicks,installs,limit_ad_tracking_installs,reattributions,sessions,cohort_revenue,revenue,daus,waus,maus,gdpr_forgets"

	payload = {"start_date" : start, "end_date" : finish,  "kpis" : kpis , "event_kpis" : events_tokens,
	          "grouping" : "day, networks, campaigns, adgroups, creatives, country, os_name",
	            }

	#get requests for sessions, clicks, installs
	response_leg = rq.get("https://api.adjust.com/kpis/v1/" + app_token_leg + ".csv", headers = user_token_leg, params = payload)
	status = response_leg.status_code
	print("Legacy:")
	print(status)

	#creates a datframe the get responces for sessions, clicks, installs
	data_l = pd.read_csv(io.StringIO(response.text))
	data_legacy = pd.DataFrame(data)

	#sets the coloum names from the defaul deliverables csv
	data_legacy = ['date', 'tracker_token', 'network', 'campaign', 'adgroup', 
                               'creative', 'country', 'os_name', 'impressions', 'clicks', 
                               'installs', 'limit_ad_tracking_installs', 'reattributions', 
                               'sessions', 'cohort_revenue', 'revenue', 'daus', 'waus', 
                               'maus', 'gdpr_forgets', 'age_group_greater_than_3_udctlp_events', 
                               'age_provided_9okrzo_events', 'assessment_1_done_awwr8i_events', 
                               'assessment_2_done_bja0fi_events', 'assessment_3_done_gsblgn_events', 
                               'assessment_4_done_pcw1dl_events', 'checkout_seen_ukw0nk_events', 
                               'end_onboarding_g7rad1_events', 'package_selected_gvcwfs_events', 
                               'purchase_8m98yt_events', 'registration_done_w2teee_events', 
                               'session_1_done_nn5l8r_events', 'session_1_started_sr4sdl_events', 
                               'start_onboarding_3nxr3f_events', 'training_intensity_selected_buuugy_events', 
                               'welcome_screen_seen_pwqi9z_events', 's_story_open_read_more_gswco5_events', 
                               'age_group_greater_than_3_udctlp_revenue', 'age_provided_9okrzo_revenue', 
                               'assessment_1_done_awwr8i_revenue', 'assessment_2_done_bja0fi_revenue', 
                               'assessment_3_done_gsblgn_revenue', 'assessment_4_done_pcw1dl_revenue', 
                               'checkout_seen_ukw0nk_revenue', 'end_onboarding_g7rad1_revenue', 
                               'package_selected_gvcwfs_revenue', 'purchase_8m98yt_revenue', 
                               'registration_done_w2teee_revenue', 'session_1_done_nn5l8r_revenue', 
                               'session_1_started_sr4sdl_revenue', 'start_onboarding_3nxr3f_revenue', 
                               'training_intensity_selected_buuugy_revenue', 'welcome_screen_seen_pwqi9z_revenue', 
                               's_story_open_read_more_gswco5_revenue']


	return data_adjust, data_legacy
