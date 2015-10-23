#!/usr/bin/python
import psycopg2
import pprint
import requests
import xmltodict, json
import urllib
import datetime
import csv
import threading
from threading import Thread
import sys, traceback

reload(sys);
sys.setdefaultencoding("utf-8");
now = datetime.datetime.now();
url = "https://service.leads360.com/ClientService.asmx/GetLeadIds?username=XXXX@sunedison.com&password=XXXX?&from=01/01/2000&to=%s" %now.strftime("%m/%d/%Y");
print now.strftime("%m/%d/%Y");

actionLogCSV = open('leadsData/actionLogCSV.csv','w');
#assignmentLogCSV = open('leadsData/assignmentLogCSV.csv','w');
#creationLogCSV = open('leadsData/creationLogCSV.csv','w');
#distributionLogCSV = open('leadsData/distributionLogCSV.csv','w');
#exportLogCSV = open('leadsData/exportLogCSV.csv','w');
#emailLogCSV = open('leadsData/emailLogCSV.csv','w');
leadFieldsCSV = open('leadsData/leadsCSV.csv','w');
#statusLogCSV = open('leadsData/statusLogCSV.csv','w');

def getLeadIds():
	r = requests.get(url);
	output = xmltodict.parse(r.content);
	for lead in output["Leads"]["Lead"]:	
		getLeadInfo(lead["@Id"]);

def getLeadInfo(id):
	urlForLeadInfo = "https://service.leads360.com/ClientService.asmx/GetLead?username=XXXX@sunedison.com&password=XXXX?&leadId=%s" %id;
	r = requests.get(urlForLeadInfo);
	output = xmltodict.parse(r.content);
	if __name__ == '__main__':
		'''try:
			Thread(target = getCreationLog(output)).start();
		except:
			print "Exception." 
		try: 
			a = threading.Thread(target = getActionLog(output));
		except:
			print "Exception."
			traceback.print_exc(file=sys.stdout);
		try:	
			Thread(target = getAssignmentLog(output)).start();
		except:
			print "Exception."
		try:
			Thread(target = getDistributionLog(output)).start();
		except:
			print "Exception."
		try:
			Thread(target = getExportLog(output)).start();
		except:
			print "Exception."
		try:
			Thread(target = getEmailLog(output)).start();
		except:
			print "Exception." 
		try:
			b = threading.Thread(target = getLeadFields(output));
		except:
			print "Exception."
			traceback.print_exc(file=sys.stdout)
		try:
			Thread(target = getStatusLog(output)).start();
		except:
			print "Exception." '''

		'''a.start();
		b.start();	

		a.join();	
		b.join();		'''

		getActionLog(output);
		getLeadFields(output);


def getActionLog(output):
	AgentName = "";
	ActionTypeName = "";
	ActionNote = "";
	GroupName = "";
	AgentEmail = "";
	ActionDate = "";
	if "ActionLog" in output["Leads"]["Lead"]["Logs"]:
		if isinstance(output["Leads"]["Lead"]["Logs"]["ActionLog"]["Action"], list):
			for action in output["Leads"]["Lead"]["Logs"]["ActionLog"]["Action"]:
				if '@AgentName' in action:
					AgentName = action["@AgentName"];
				else:
					AgentName = "";

				if '@ActionTypeName' in action:
					ActionTypeName = action["@ActionTypeName"];
				else:
					ActionTypeName = "";

				if '@ActionNote' in action:
					ActionNote = action["@ActionNote"];
				else:
					ActionNote = "";

				if '@GroupName' in action:
					GroupName = action["@GroupName"];
				else:
					GroupName = "";

				if '@AgentEmail' in action:
					AgentEmail = action["@AgentEmail"];
				else:
					AgentEmail = "";

				if '@ActionDate' in action:
					ActionDate = action["@ActionDate"];
				else:
					ActionDate = "";

				id = output["Leads"]["Lead"]["@Id"];
				CreateDate = output["Leads"]["Lead"]["@CreateDate"];
				ModifyDate = output["Leads"]["Lead"]["@ModifyDate"];

				generateActionLogCsv(AgentName,ActionTypeName,ActionNote,GroupName,AgentEmail,ActionDate,id,CreateDate,ModifyDate);
		else:
			if '@AgentName' in output["Leads"]["Lead"]["Logs"]["ActionLog"]["Action"]:
				AgentName = output["Leads"]["Lead"]["Logs"]["ActionLog"]["Action"]["@AgentName"];
			else:
				AgentName = "";

			if '@ActionTypeName' in output["Leads"]["Lead"]["Logs"]["ActionLog"]["Action"]:
				ActionTypeName = output["Leads"]["Lead"]["Logs"]["ActionLog"]["Action"]["@ActionTypeName"];
			else:
				ActionTypeName = "";

			if '@ActionNote' in output["Leads"]["Lead"]["Logs"]["ActionLog"]["Action"]:
				ActionNote = output["Leads"]["Lead"]["Logs"]["ActionLog"]["Action"]["@ActionNote"];
			else:
				ActionNote = "";

			if '@GroupName' in output["Leads"]["Lead"]["Logs"]["ActionLog"]["Action"]:
				GroupName = output["Leads"]["Lead"]["Logs"]["ActionLog"]["Action"]["@GroupName"];
			else:
				GroupName = "";

			if '@AgentEmail' in output["Leads"]["Lead"]["Logs"]["ActionLog"]["Action"]:
				AgentEmail = output["Leads"]["Lead"]["Logs"]["ActionLog"]["Action"]["@AgentEmail"];
			else:
				AgentEmail = "";

			if '@ActionDate' in output["Leads"]["Lead"]["Logs"]["ActionLog"]["Action"]:
				ActionDate = output["Leads"]["Lead"]["Logs"]["ActionLog"]["Action"]["@ActionDate"];
			else:
				ActionDate = "";		

			id = output["Leads"]["Lead"]["@Id"];
			CreateDate = output["Leads"]["Lead"]["@CreateDate"];
			ModifyDate = output["Leads"]["Lead"]["@ModifyDate"];

			generateActionLogCsv(AgentName,ActionTypeName,ActionNote,GroupName,AgentEmail,ActionDate,id,CreateDate,ModifyDate);

def getAssignmentLog(output):
	AssignedAgentName = "";
	AssignedByAgentName = "";
	AssignedByGroupName = "";
	AssignedGroupName = "";
	AssignedByAgentEmail = "";
	AssignedAgentEmail = "";
	LogDate = "";
	if "AssignmentLog" in output["Leads"]["Lead"]["Logs"]:
		if isinstance(output["Leads"]["Lead"]["Logs"]["AssignmentLog"]["Assignment"], list):
			for assignment in output["Leads"]["Lead"]["Logs"]["AssignmentLog"]["Assignment"]:
				if '@AssignedAgentName' in assignment:
					AssignedAgentName = assignment["@AssignedAgentName"];
				else:
					AssignedAgentName = "";

				if '@AssignedByAgentName' in assignment:
					AssignedByAgentName = assignment["@AssignedByAgentName"];
				else:
					AssignedByAgentName = "";

				if '@AssignedByGroupName' in assignment:
					AssignedByGroupName = assignment["@AssignedByGroupName"];
				else:
					AssignedByGroupName = "";

				if '@AssignedGroupName' in assignment:
					AssignedGroupName = assignment["@AssignedGroupName"];
				else:
					AssignedGroupName = "";

				if '@AssignedByAgentEmail' in assignment:
					AssignedByAgentEmail = assignment["@AssignedByAgentEmail"];
				else:
					AssignedByAgentEmail = "";

				if '@AssignedAgentEmail' in assignment:
					AssignedAgentEmail = assignment["@AssignedAgentEmail"];
				else:
					AssignedAgentEmail = "";

				if '@LogDate' in assignment:
					LogDate = assignment['@LogDate'];
				else:
					LogDate = "";

				id = output["Leads"]["Lead"]["@Id"];
				CreateDate = output["Leads"]["Lead"]["@CreateDate"];
				ModifyDate = output["Leads"]["Lead"]["@ModifyDate"];

				generateAssignmentLogCsv(AssignedAgentName,AssignedByAgentName,AssignedByGroupName,AssignedGroupName,AssignedByAgentEmail,AssignedAgentEmail,LogDate,id,CreateDate,ModifyDate);
		else:
			if '@AssignedAgentName' in output["Leads"]["Lead"]["Logs"]["AssignmentLog"]["Assignment"]:
				AssignedAgentName = output["Leads"]["Lead"]["Logs"]["AssignmentLog"]["Assignment"]["@AssignedAgentName"];
			else:
				AssignedAgentName = "";

			if '@AssignedByAgentName' in output["Leads"]["Lead"]["Logs"]["AssignmentLog"]["Assignment"]:
				AssignedByAgentName = output["Leads"]["Lead"]["Logs"]["AssignmentLog"]["Assignment"]["@AssignedByAgentName"];
			else:
				AssignedByAgentName = "";

			if '@AssignedByGroupName' in output["Leads"]["Lead"]["Logs"]["AssignmentLog"]["Assignment"]:
				AssignedByGroupName = output["Leads"]["Lead"]["Logs"]["AssignmentLog"]["Assignment"]["@AssignedByGroupName"];
			else:
				AssignedByGroupName = "";

			if '@AssignedGroupName' in output["Leads"]["Lead"]["Logs"]["AssignmentLog"]["Assignment"]:
				AssignedGroupName = output["Leads"]["Lead"]["Logs"]["AssignmentLog"]["Assignment"]["@AssignedGroupName"];
			else:
				AssignedGroupName = "";

			if '@AssignedByAgentEmail' in output["Leads"]["Lead"]["Logs"]["AssignmentLog"]["Assignment"]:
				AssignedByAgentEmail = output["Leads"]["Lead"]["Logs"]["AssignmentLog"]["Assignment"]["@AssignedByAgentEmail"];
			else:
				AssignedByAgentEmail = "";

			if '@AssignedAgentEmail' in output["Leads"]["Lead"]["Logs"]["AssignmentLog"]["Assignment"]:
				AssignedAgentEmail = output["Leads"]["Lead"]["Logs"]["AssignmentLog"]["Assignment"]["@AssignedAgentEmail"];
			else:
				AssignedAgentEmail = "";	

			if '@LogDate' in output["Leads"]["Lead"]["Logs"]["AssignmentLog"]["Assignment"]:
				LogDate = output["Leads"]["Lead"]["Logs"]["AssignmentLog"]["Assignment"]["@LogDate"];
			else:
				LogDate = "";

			id = output["Leads"]["Lead"]["@Id"];
			CreateDate = output["Leads"]["Lead"]["@CreateDate"];
			ModifyDate = output["Leads"]["Lead"]["@ModifyDate"];

			generateAssignmentLogCsv(AssignedAgentName,AssignedByAgentName,AssignedByGroupName,AssignedGroupName,AssignedByAgentEmail,AssignedAgentEmail,LogDate,id,CreateDate,ModifyDate);

def getCreationLog(output):
	AssignedAgentName = "";
	CreatedByAgentName = "";
	CreatedByAgentEmail = "";
	AssignedGroupName = "";
	AssignedAgentEmail = "";
	CreatedByGroupName = "";
	LogDate = "";
	if "CreationLog" in output["Leads"]["Lead"]["Logs"]:
		if '@AssignedAgentName' in output["Leads"]["Lead"]["Logs"]["CreationLog"]:
			AssignedAgentName = output["Leads"]["Lead"]["Logs"]["CreationLog"]["@AssignedAgentName"];
		else:
			AssignedAgentName = "";

		if '@CreatedByAgentName' in output["Leads"]["Lead"]["Logs"]["CreationLog"]:
			CreatedByAgentName = output["Leads"]["Lead"]["Logs"]["CreationLog"]["@CreatedByAgentName"];
		else:
			CreatedByAgentName = "";

		if '@CreatedByAgentEmail' in output["Leads"]["Lead"]["Logs"]["CreationLog"]:
			CreatedByAgentEmail = output["Leads"]["Lead"]["Logs"]["CreationLog"]["@CreatedByAgentEmail"];
		else:
			CreatedByAgentEmail = "";

		if '@AssignedGroupName' in output["Leads"]["Lead"]["Logs"]["CreationLog"]:
			AssignedGroupName = output["Leads"]["Lead"]["Logs"]["CreationLog"]["@AssignedGroupName"];
		else:
			AssignedGroupName = "";

		if '@AssignedAgentEmail' in output["Leads"]["Lead"]["Logs"]["CreationLog"]:
			AssignedAgentEmail = output["Leads"]["Lead"]["Logs"]["CreationLog"]["@AssignedAgentEmail"];
		else:
			AssignedAgentEmail = "";

		if '@CreatedByGroupName' in output["Leads"]["Lead"]["Logs"]["CreationLog"]:
			CreatedByGroupName = output["Leads"]["Lead"]["Logs"]["CreationLog"]["@CreatedByGroupName"];
		else:
			CreatedByGroupName = "";

		if '@LogDate' in output["Leads"]["Lead"]["Logs"]["CreationLog"]:
			LogDate = output["Leads"]["Lead"]["Logs"]["CreationLog"]["@LogDate"];
		else:
			LogDate = "";

		id = output["Leads"]["Lead"]["@Id"];
		CreateDate = output["Leads"]["Lead"]["@CreateDate"];
		ModifyDate = output["Leads"]["Lead"]["@ModifyDate"];	

		generateCreationLogCsv(AssignedAgentName, CreatedByAgentName, CreatedByAgentEmail, AssignedGroupName, AssignedAgentEmail, CreatedByGroupName, LogDate, id, CreateDate, ModifyDate);

def getDistributionLog(output):
	AssignedAgentName = "";
	AssignedGroupName = "";
	AssignedAgentEmail = "";
	DistributionProgramName = "";
	LogDate = "";
	if "DistributionLog" in output["Leads"]["Lead"]["Logs"]:
		if isinstance(output["Leads"]["Lead"]["Logs"]["DistributionLog"]["Distribution"], list):
			for distribution in output["Leads"]["Lead"]["Logs"]["DistributionLog"]["Distribution"]:
				if '@AssignedAgentName' in distribution:
					AssignedAgentName = distribution["@AssignedAgentName"];
				else:
					AssignedAgentName = "";

				if '@AssignedGroupName' in distribution:
					AssignedGroupName = distribution["@AssignedGroupName"];
				else:
					AssignedGroupName = "";

				if '@AssignedAgentEmail' in distribution:
					AssignedAgentEmail = distribution["@AssignedAgentEmail"];
				else:
					AssignedAgentEmail = "";

				if '@DistributionProgramName' in distribution:
					DistributionProgramName = distribution["@DistributionProgramName"];
				else:
					DistributionProgramName = "";

				if '@LogDate' in distribution:
					LogDate = distribution['@LogDate'];
				else:
					LogDate = "";

				id = output["Leads"]["Lead"]["@Id"];
				CreateDate = output["Leads"]["Lead"]["@CreateDate"];
				ModifyDate = output["Leads"]["Lead"]["@ModifyDate"];

				generateDistributionLogCsv(AssignedAgentName,AssignedGroupName,AssignedAgentEmail,DistributionProgramName,LogDate,id,CreateDate,ModifyDate);
		else:
			if '@AssignedAgentName' in output["Leads"]["Lead"]["Logs"]["DistributionLog"]["Distribution"]:
				AssignedAgentName = output["Leads"]["Lead"]["Logs"]["DistributionLog"]["Distribution"]["@AssignedAgentName"];
			else:
				AssignedAgentName = "";

			if '@AssignedGroupName' in output["Leads"]["Lead"]["Logs"]["DistributionLog"]["Distribution"]:
				AssignedGroupName = output["Leads"]["Lead"]["Logs"]["DistributionLog"]["Distribution"]["@AssignedGroupName"];
			else:
				AssignedGroupName = "";

			if '@AssignedAgentEmail' in output["Leads"]["Lead"]["Logs"]["DistributionLog"]["Distribution"]:
				AssignedAgentEmail = output["Leads"]["Lead"]["Logs"]["DistributionLog"]["Distribution"]["@AssignedAgentEmail"];
			else:
				AssignedAgentEmail = "";

			if '@DistributionProgramName' in output["Leads"]["Lead"]["Logs"]["DistributionLog"]["Distribution"]:
				DistributionProgramName = output["Leads"]["Lead"]["Logs"]["DistributionLog"]["Distribution"]["@DistributionProgramName"];
			else:
				DistributionProgramName = "";	

			if '@LogDate' in output["Leads"]["Lead"]["Logs"]["DistributionLog"]["Distribution"]:
				LogDate = output["Leads"]["Lead"]["Logs"]["DistributionLog"]["Distribution"]["@LogDate"];
			else:
				LogDate = "";

			id = output["Leads"]["Lead"]["@Id"];
			CreateDate = output["Leads"]["Lead"]["@CreateDate"];
			ModifyDate = output["Leads"]["Lead"]["@ModifyDate"];

			generateDistributionLogCsv(AssignedAgentName,AssignedGroupName,AssignedAgentEmail,DistributionProgramName,LogDate,id,CreateDate,ModifyDate);

def getExportLog(output):
	AgentName = "";
	Result = "";
	Message = "";
	System = "";
	AgentEmail = "";
	LogDate = "";
	if "ExportLog" in output["Leads"]["Lead"]["Logs"]:
		if isinstance(output["Leads"]["Lead"]["Logs"]["ExportLog"]["Export"], list):
			for export in output["Leads"]["Lead"]["Logs"]["ExportLog"]["Export"]:
				if '@AgentName' in export:
					AgentName = export["@AgentName"];
				else:
					AgentName = "";

				if '@Result' in export:
					Result = export["@Result"];
				else:
					Result = "";

				if '@Message' in export:
					Message = export["@Message"];
				else:
					Message = "";

				if '@System' in export:
					System = export["@System"];
				else:
					System = "";

				if '@AgentEmail' in export:
					AgentEmail = export["@AgentEmail"];
				else:
					AgentEmail = "";

				if '@LogDate' in export:
					LogDate = export['@LogDate'];
				else:
					LogDate = "";

				id = output["Leads"]["Lead"]["@Id"];
				CreateDate = output["Leads"]["Lead"]["@CreateDate"];
				ModifyDate = output["Leads"]["Lead"]["@ModifyDate"];

				generateExportLogCsv(AgentName,Result,Message,System,AgentEmail,LogDate,id,CreateDate,ModifyDate);
		else:
			if '@AgentName' in output["Leads"]["Lead"]["Logs"]["ExportLog"]["Export"]:
				AgentName = output["Leads"]["Lead"]["Logs"]["ExportLog"]["Export"]["@AgentName"];
			else:
				AgentName = "";

			if '@Result' in output["Leads"]["Lead"]["Logs"]["ExportLog"]["Export"]:
				Result = output["Leads"]["Lead"]["Logs"]["ExportLog"]["Export"]["@Result"];
			else:
				Result = "";

			if '@Message' in output["Leads"]["Lead"]["Logs"]["ExportLog"]["Export"]:
				Message = output["Leads"]["Lead"]["Logs"]["ExportLog"]["Export"]["@Message"];
			else:
				Message = "";

			if '@System' in output["Leads"]["Lead"]["Logs"]["ExportLog"]["Export"]:
				System = output["Leads"]["Lead"]["Logs"]["ExportLog"]["Export"]["@System"];
			else:
				System = "";	

			if '@AgentEmail' in output["Leads"]["Lead"]["Logs"]["ExportLog"]["Export"]:
				AgentEmail = output["Leads"]["Lead"]["Logs"]["ExportLog"]["Export"]["@AgentEmail"];
			else:
				AgentEmail = "";

			if '@LogDate' in output["Leads"]["Lead"]["Logs"]["ExportLog"]["Export"]:
				LogDate = output["Leads"]["Lead"]["Logs"]["ExportLog"]["Export"]["@LogDate"];
			else:
				LogDate = "";

			id = output["Leads"]["Lead"]["@Id"];
			CreateDate = output["Leads"]["Lead"]["@CreateDate"];
			ModifyDate = output["Leads"]["Lead"]["@ModifyDate"];

			generateExportLogCsv(AgentName,Result,Message,System,AgentEmail,LogDate,id,CreateDate,ModifyDate);

def getEmailLog(output):
	AgentName = "";
	SendDate = "";
	EmailTemplateName = "";
	if "EmailLog" in output["Leads"]["Lead"]["Logs"]:
		if isinstance(output["Leads"]["Lead"]["Logs"]["EmailLog"]["Email"], list):
			for email in output["Leads"]["Lead"]["Logs"]["EmailLog"]["Email"]:
				if '@AgentName' in email:
					AgentName = email["@AgentName"];
				else:
					AgentName = "";

				if '@SendDate' in email:
					SendDate = email["@SendDate"];
				else:
					SendDate = "";

				if '@EmailTemplateName' in email:
					EmailTemplateName = email["@EmailTemplateName"];
				else:
					EmailTemplateName = "";

				id = output["Leads"]["Lead"]["@Id"];
				CreateDate = output["Leads"]["Lead"]["@CreateDate"];
				ModifyDate = output["Leads"]["Lead"]["@ModifyDate"];

				generateEmailLogCsv(AgentName,SendDate,EmailTemplateName,id,CreateDate,ModifyDate);
		else:
			if '@AgentName' in output["Leads"]["Lead"]["Logs"]["EmailLog"]["Email"]:
				AgentName = output["Leads"]["Lead"]["Logs"]["EmailLog"]["Email"]["@AgentName"];
			else:
				AgentName = "";

			if '@SendDate' in output["Leads"]["Lead"]["Logs"]["EmailLog"]["Email"]:
				SendDate = output["Leads"]["Lead"]["Logs"]["EmailLog"]["Email"]["@SendDate"];
			else:
				SendDate = "";

			if '@EmailTemplateName' in output["Leads"]["Lead"]["Logs"]["EmailLog"]["Email"]:
				EmailTemplateName = output["Leads"]["Lead"]["Logs"]["EmailLog"]["Email"]["@EmailTemplateName"];
			else:
				EmailTemplateName = "";

			id = output["Leads"]["Lead"]["@Id"];
			CreateDate = output["Leads"]["Lead"]["@CreateDate"];
			ModifyDate = output["Leads"]["Lead"]["@ModifyDate"];

			generateEmailLogCsv(AgentName,SendDate,EmailTemplateName,id,CreateDate,ModifyDate);

def getLeadFields(output):
	firstname="";
	lastname="";
	home_address="";
	city="";
	state="";
	zip="";
	home_phone="";
	work_phone="";
	email="";
	average_monthly_electric_bill="";
	electricity_utility_company="";
	do_you_own_the_home="";
	roof_shade="";
	roof_type="";
	google_street_view_link="";
	sms_phone="";
	sms_opt_out="";
	netsuite_id="";
	lead_source="";
	lead_price_paid="";
	country="";
	duplicate_result_from_netsuite="";
	home_type="";
	preferred_method_of_follow_up="";
	lead_generator_comment="";
	lead_generator_date="";
	lead_generator_time="";
	updates_about_sunedison="";
	how_much_do_you_want_to_cut_your_bill="";
	homeowner_require_financing="";
	water_heater_type="";
	interested_in="";
	originator="";
	originator_email="";
	sunedison_employee_referrer_firstname="";
	sunedison_employee_referrer_lastname="";
	sunedison_employee_referrer_email="";
	does_your_house_have_an_attic="";
	do_you_have_a_central_heating_and_cooling_system="";
	primary_phone="";
	credit_score="";
	custom_1="";
	adressee="";
	total_system_size_echowatts="";
	total_retail_price_quoted="";
	contract_cancelled_date="";
	interested_in_purchase_type="";
	financing_program="";
	solar_designer="";
	lost_reason="";
	lost="";
	lost_cancel_comments="";
	homeowner_lease_contract_status="";
	homeowner_credit_check_status="";
	address_2="";
	proposal_status="";
	campaign_subcategory="";
	name="";
	address="";
	google_street_view_link1="";
	email1="";
	primary_phone1="";
	home_phone1="";
	mobile_phone1="";
	work_phone1="";
	international_phone="";
	netsuite_token_id="";
	advisor_notes="";
	solar_advisor="";
	kw_sold="";
	system_price="";
	energy_consultant="";
	energy_consultant_notes="";
	other_notes="";
	lead_source_manual="";
	designer_notes="";
	pricing_exceptions="";
	id="";
	collected_utility_bill="";
	contract_status="";
	createdate="";
	modifydate="";
	last_distribution_date="";
	lead_title="";
	status="";
	agent_name="";
	agent_email="";
	campaign="";

	if isinstance(output["Leads"]["Lead"]["Fields"]["Field"], list):
		for lead in output["Leads"]["Lead"]["Fields"]["Field"]:
			if lead['@FieldTitle'] == 'First Name':
				firstname = lead['@Value'];
			elif lead['@FieldTitle'] == 'Last Name':
				lastname = lead['@Value'];
			elif lead['@FieldTitle'] == 'Home Address':
				home_address = lead['@Value'];
			elif lead['@FieldTitle'] == 'City':
				city = lead['@Value'];
			elif lead['@FieldTitle'] == 'State':
				state = lead['@Value'];
			elif lead['@FieldTitle'] == 'Zip':
				zip = lead['@Value'];
			elif lead['@FieldTitle'] == 'Home Phone':
				home_phone = lead['@Value'];
			elif lead['@FieldTitle'] == 'Work Phone':
				work_phone = lead['@Value'];
			elif lead['@FieldTitle'] == 'Email':
				email = lead['@Value'];
			elif lead['@FieldTitle'] == 'Average Monthly Electric Bill':
				average_monthly_electric_bill = lead['@Value'];
			elif lead['@FieldTitle'] == 'Electricity Utility Company':
				electricity_utility_company = lead['@Value'];
			elif lead['@FieldTitle'] == 'Do you own the home?':
				do_you_own_the_home = lead['@Value'];
			elif lead['@FieldTitle'] == 'Roof Shade':
				roof_shade = lead['@Value'];
			elif lead['@FieldTitle'] == 'Roof Type':
				roof_type = lead['@Value'];
			elif lead['@FieldTitle'] == 'Google Street View Link':
				google_street_view_link = lead['@Value'];
			elif lead['@FieldTitle'] == 'SMS Phone':
				sms_phone = lead['@Value'];
			elif lead['@FieldTitle'] == 'SMS Opt-Out':
				sms_opt_out = lead['@Value'];
			elif lead['@FieldTitle'] == 'Netsuite ID':
				netsuite_id = lead['@Value'];
			elif lead['@FieldTitle'] == 'Lead Source':
				lead_source = lead['@Value'];
			elif lead['@FieldTitle'] == 'Lead Price Paid ($)':
				lead_price_paid = lead['@Value'];
			elif lead['@FieldTitle'] == 'Country':
				country = lead['@Value'];
			elif lead['@FieldTitle'] == 'Duplicate Result from Netsuite':
				duplicate_result_from_netsuite = lead['@Value'];
			elif lead['@FieldTitle'] == 'Home Type':
				home_type = lead['@Value'];
			elif lead['@FieldTitle'] == 'Preferred Method of Follow-Up':
				preferred_method_of_follow_up = lead['@Value'];
			elif lead['@FieldTitle'] == 'Lead Generator Comment':
				lead_generator_comment = lead['@Value'];
			elif lead['@FieldTitle'] == 'Lead Generator Date':
				lead_generator_date = lead['@Value'];
			elif lead['@FieldTitle'] == 'Lead Generator Time':
				lead_generator_time = lead['@Value'];
			elif lead['@FieldTitle'] == 'Yes, I would like to receive updates about SunEdis':
				updates_about_sunedison = lead['@Value'];
			elif lead['@FieldTitle'] == 'How much do you want to cut your bill?':
				how_much_do_you_want_to_cut_your_bill = lead['@Value'];
			elif lead['@FieldTitle'] == 'Homeowner require financing?':
				homeowner_require_financing = lead['@Value'];
			elif lead['@FieldTitle'] == 'Water Heater Type':
				water_heater_type = lead['@Value'];
			elif lead['@FieldTitle'] == 'Interested In':
				interested_in = lead['@Value'];
			elif lead['@FieldTitle'] == 'Originator':
				originator = lead['@Value'];
			elif lead['@FieldTitle'] == 'Originator Email':
				originator_email = lead['@Value'];
			elif lead['@FieldTitle'] == 'SunEdison Employee Referrer First Name':
				sunedison_employee_referrer_firstname = lead['@Value'];
			elif lead['@FieldTitle'] == 'SunEdison Employee Referrer Last Name':
				sunedison_employee_referrer_lastname = lead['@Value'];
			elif lead['@FieldTitle'] == 'SunEdison Employee Referrer Email':
				sunedison_employee_referrer_email = lead['@Value'];
			elif lead['@FieldTitle'] == 'Does your house have an attic?':
				does_your_house_have_an_attic = lead['@Value'];
			elif lead['@FieldTitle'] == 'Do you have a central heating and cooling system?':
				do_you_have_a_central_heating_and_cooling_system = lead['@Value'];
			elif lead['@FieldTitle'] == 'Primary Phone':
				primary_phone = lead['@Value'];
			elif lead['@FieldTitle'] == 'Credit Score':
				credit_score = lead['@Value'];
			elif lead['@FieldTitle'] == 'Custom 1':
				custom_1 = lead['@Value'];
			elif lead['@FieldTitle'] == 'Adressee':
				adressee = lead['@Value'];
			elif lead['@FieldTitle'] == 'Total System Size (Echo Watts)':
				total_system_size_echowatts = lead['@Value'];
			elif lead['@FieldTitle'] == 'Total Retail Price Quoted':
				total_retail_price_quoted = lead['@Value'];
			elif lead['@FieldTitle'] == 'Contract Cancelled Date':
				contract_cancelled_date = lead['@Value'];
			elif lead['@FieldTitle'] == 'Interested in Purchase Type':
				interested_in_purchase_type = lead['@Value'];
			elif lead['@FieldTitle'] == 'Financing Program':
				financing_program = lead['@Value'];
			elif lead['@FieldTitle'] == 'Solar Designer':
				solar_designer = lead['@Value'];
			elif lead['@FieldTitle'] == 'Lost Reason':
				lost_reason = lead['@Value'];
			elif lead['@FieldTitle'] == 'Lost':
				Lost = lead['@Value'];
			elif lead['@FieldTitle'] == 'Lost/Cancel Comments':
				lost_cancel_comments = lead['@Value'];
			elif lead['@FieldTitle'] == 'Homeowner Lease Contract Status':
				homeowner_lease_contract_status = lead['@Value'];
			elif lead['@FieldTitle'] == 'Homeowner Credit Check Status':
				homeowner_credit_check_status = lead['@Value'];
			elif lead['@FieldTitle'] == 'Proposal Status':
				proposal_status = lead['@Value'];
			elif lead['@FieldTitle'] == 'Campaign SubCategory':
				campaign_subcategory = lead['@Value'];
			elif lead['@FieldTitle'] == 'Name':
				name = lead['@Value'];
			elif lead['@FieldTitle'] == 'Address':
				address = lead['@Value'];
			elif lead['@FieldTitle'] == 'Google Street View Link1':
				google_street_view_link1 = lead['@Value'];
			elif lead['@FieldTitle'] == 'Email1':
				email1 = lead['@Value'];
			elif lead['@FieldTitle'] == 'Primary Phone1':
				primary_phone1 = lead['@Value'];
			elif lead['@FieldTitle'] == 'Home Phone1':
				home_phone1 = lead['@Value'];
			elif lead['@FieldTitle'] == 'Mobile Phone1':
				mobile_phone1 = lead['@Value'];
			elif lead['@FieldTitle'] == 'Work Phone1':
				work_phone1 = lead['@Value'];
			elif lead['@FieldTitle'] == 'International Phone':
				international_phone = lead['@Value'];
			elif lead['@FieldTitle'] == 'Netsuite Token ID':
				netsuite_token_id = lead['@Value'];
			elif lead['@FieldTitle'] == 'Advisor Notes':
				advisor_notes = lead['@Value'];
			elif lead['@FieldTitle'] == 'Solar Advisor':
				solar_advisor = lead['@Value'];
			elif lead['@FieldTitle'] == 'kW Sold':
				kw_sold = lead['@Value'];
			elif lead['@FieldTitle'] == 'System Price':
				system_price = lead['@Value'];
			elif lead['@FieldTitle'] == 'Energy Consultant':
				energy_consultant = lead['@Value'];
			elif lead['@FieldTitle'] == 'Energy Consultant Notes':
				energy_consultant_notes = lead['@Value'];
			elif lead['@FieldTitle'] == 'Other Notes':
				other_notes = lead['@Value'];
			elif lead['@FieldTitle'] == 'Lead Source Manual':
				lead_source_manual = lead['@Value'];
			elif lead['@FieldTitle'] == 'Designer Notes':
				designer_notes = lead['@Value'];
			elif lead['@FieldTitle'] == 'Pricing Exceptions':
				pricing_exceptions = lead['@Value'];
			elif lead['@FieldTitle'] == 'Velocify Lead ID':
				id = lead['@Value'];
			elif lead['@FieldTitle'] == 'Collected Utility Bill':
				collected_utility_bill = lead['@Value'];
			elif lead['@FieldTitle'] == 'Contract Status':
				contract_status = lead['@Value'];

		createdate = output["Leads"]["Lead"]["@CreateDate"];
		modifydate = output["Leads"]["Lead"]["@ModifyDate"];
		last_distribution_date = output["Leads"]["Lead"]["@LastDistributionDate"];
		lead_title = output["Leads"]["Lead"]["@LeadTitle"];
		status = output["Leads"]["Lead"]["Status"]["@StatusTitle"];
		if 'Agent' in output["Leads"]["Lead"]:
			agent_name = output["Leads"]["Lead"]["Agent"]["@AgentName"];
			agent_email = output["Leads"]["Lead"]["Agent"]["@AgentEmail"];
		campaign = output["Leads"]["Lead"]["Campaign"]["@CampaignTitle"];

		generateLeadFieldsCsv(firstname,lastname,home_address,city,state,zip,home_phone,work_phone,email,average_monthly_electric_bill,electricity_utility_company,do_you_own_the_home,roof_shade,roof_type,google_street_view_link,sms_phone,sms_opt_out,netsuite_id,lead_source,lead_price_paid,country,duplicate_result_from_netsuite,home_type,preferred_method_of_follow_up,lead_generator_comment,lead_generator_date,lead_generator_time,updates_about_sunedison,how_much_do_you_want_to_cut_your_bill,homeowner_require_financing,water_heater_type,interested_in,originator,originator_email,sunedison_employee_referrer_firstname,sunedison_employee_referrer_lastname,sunedison_employee_referrer_email,does_your_house_have_an_attic,do_you_have_a_central_heating_and_cooling_system,primary_phone,credit_score,custom_1,adressee,total_system_size_echowatts,total_retail_price_quoted,contract_cancelled_date,interested_in_purchase_type,financing_program,solar_designer,lost_reason,lost,lost_cancel_comments,homeowner_lease_contract_status,homeowner_credit_check_status,address_2,proposal_status,campaign_subcategory,name,address,google_street_view_link1,email1,primary_phone1,home_phone1,mobile_phone1,work_phone1,international_phone,netsuite_token_id,advisor_notes,solar_advisor,kw_sold,system_price,energy_consultant,energy_consultant_notes,other_notes,lead_source_manual,designer_notes,pricing_exceptions,id,collected_utility_bill,contract_status,createdate,modifydate,last_distribution_date,lead_title,status,agent_name,agent_email,campaign);

	else:
		print "Not needed to be inserted into Redshift!!"	

def getStatusLog(output):
	AgentName = "";
	GroupName = ""; 
	StatusTitle = "";
	AgentEmail = "";
	LogDate = "";
	if "StatusLog" in output["Leads"]["Lead"]["Logs"]:
		if isinstance(output["Leads"]["Lead"]["Logs"]["StatusLog"]["Status"], list):
			for status in output["Leads"]["Lead"]["Logs"]["StatusLog"]["Status"]:
				if '@AgentName' in status:
					AgentName = status["@AgentName"];
				else:
					AgentName = "";

				if '@GroupName' in status:
					GroupName = status["@GroupName"];
				else:
					GroupName = "";

				if '@StatusTitle' in status:
					StatusTitle = status["@StatusTitle"];
				else:
					StatusTitle = "";

				if '@AgentEmail' in status:
					AgentEmail = status["@AgentEmail"];
				else:
					AgentEmail = "";

				if '@LogDate' in status:
					LogDate = status["@LogDate"];
				else:
					LogDate = "";

				id = output["Leads"]["Lead"]["@Id"];
				CreateDate = output["Leads"]["Lead"]["@CreateDate"];
				ModifyDate = output["Leads"]["Lead"]["@ModifyDate"];

				generateStatusLogCsv(AgentName,GroupName,StatusTitle,AgentEmail,LogDate,id,CreateDate,ModifyDate);
		else:
			if '@AgentName' in output["Leads"]["Lead"]["Logs"]["StatusLog"]["Status"]:
				AgentName = output["Leads"]["Lead"]["Logs"]["StatusLog"]["Status"]["@AgentName"];
			else:
				AgentName = "";

			if '@GroupName' in output["Leads"]["Lead"]["Logs"]["StatusLog"]["Status"]:
				GroupName = output["Leads"]["Lead"]["Logs"]["StatusLog"]["Status"]["@GroupName"];
			else:
				GroupName = "";

			if '@StatusTitle' in output["Leads"]["Lead"]["Logs"]["StatusLog"]["Status"]:
				StatusTitle = output["Leads"]["Lead"]["Logs"]["StatusLog"]["Status"]["@StatusTitle"];
			else:
				StatusTitle = "";

			if '@AgentEmail' in output["Leads"]["Lead"]["Logs"]["StatusLog"]["Status"]:
				AgentEmail = output["Leads"]["Lead"]["Logs"]["StatusLog"]["Status"]["@AgentEmail"];
			else:
				AgentEmail = "";

			if '@LogDate' in output["Leads"]["Lead"]["Logs"]["StatusLog"]["Status"]:
				LogDate = output["Leads"]["Lead"]["Logs"]["StatusLog"]["Status"]["@LogDate"];
			else:
				LogDate = "";

			id = output["Leads"]["Lead"]["@Id"];
			CreateDate = output["Leads"]["Lead"]["@CreateDate"];
			ModifyDate = output["Leads"]["Lead"]["@ModifyDate"];

			generateStatusLogCsv(AgentName,GroupName,StatusTitle,AgentEmail,LogDate,id,CreateDate,ModifyDate);

def generateCreationLogCsv(AssignedAgentName, CreatedByAgentName, CreatedByAgentEmail, AssignedGroupName, AssignedAgentEmail, CreatedByGroupName, LogDate, id, CreateDate, ModifyDate):
	creationLogVariables = "^" + AssignedAgentName + "^,^" + CreatedByAgentName + "^,^" + CreatedByAgentEmail + "^,^" + AssignedGroupName + "^,^" + AssignedAgentEmail + "^,^" + CreatedByGroupName + "^,^" + LogDate + "^,^" + id + "^,^" + CreateDate + "^,^" + ModifyDate + "^\n";
	creationLogCSV.write(creationLogVariables);
	print("Data written for ID(CreationLog): %s" %id);

def generateActionLogCsv(AgentName,ActionTypeName,ActionNote,GroupName,AgentEmail,ActionDate,id,CreateDate,ModifyDate):
	actionLogVariables = "<" + AgentName + "<,<" + ActionTypeName + "<,<" + ActionNote + "<,<" + GroupName + "<,<" + AgentEmail + "<,<" + ActionDate + "<,<" + id + "<,<" + CreateDate + "<,<" + ModifyDate + "<\n";
	actionLogCSV.write(actionLogVariables);
	print("Data written for ID(ActionLog): %s" %id);

def generateAssignmentLogCsv(AssignedAgentName,AssignedByAgentName,AssignedByGroupName,AssignedGroupName,AssignedByAgentEmail,AssignedAgentEmail,LogDate,id,CreateDate,ModifyDate):
	assignmentLogVariables = "^" + AssignedAgentName + "^,^" + AssignedByAgentName + "^,^" + AssignedByGroupName + "^,^" + AssignedGroupName + "^,^" + AssignedByAgentEmail + "^,^" + AssignedAgentEmail + "^,^" + LogDate + "^,^" + id + "^,^" + CreateDate + "^,^" + ModifyDate + "^\n";
	assignmentLogCSV.write(assignmentLogVariables);
	print("Data written for ID(AssignmentLog): %s" %id);

def generateDistributionLogCsv(AssignedAgentName,AssignedGroupName,AssignedAgentEmail,DistributionProgramName,LogDate,id,CreateDate,ModifyDate):
	distributionLogVariables = "^" + AssignedAgentName + "^,^" + AssignedGroupName + "^,^" + AssignedAgentEmail + "^,^" + DistributionProgramName + "^,^" + LogDate + "^,^" + id + "^,^" + CreateDate + "^,^" + ModifyDate + "^\n";
	distributionLogCSV.write(distributionLogVariables);
	print("Data written for ID(DistributionLog): %s" %id);

def generateExportLogCsv(AgentName,Result,Message,System,AgentEmail,LogDate,id,CreateDate,ModifyDate):
	exportLogVariables = "^" + AgentName + "^,^" + Result + "^,^" + Message + "^,^" + System + "^,^" + AgentEmail +  "^,^" + LogDate + "^,^" + id + "^,^" + CreateDate + "^,^" + ModifyDate + "^\n";
	exportLogCSV.write(exportLogVariables);
	print("Data written for ID(ExportLog): %s" %id);

def generateEmailLogCsv(AgentName,SendDate,EmailTemplateName,id,CreateDate,ModifyDate):
	emailLogVariables = "^" + AgentName + "^,^" + SendDate + "^,^" + EmailTemplateName + "^,^" + id + "^,^" + CreateDate + "^,^" + ModifyDate + "^\n";
	emailLogCSV.write(emailLogVariables);
	print("Data written for ID(EmailLog): %s" %id);

def generateLeadFieldsCsv(firstname,lastname,home_address,city,state,zip,home_phone,work_phone,email,average_monthly_electric_bill,electricity_utility_company,do_you_own_the_home,roof_shade,roof_type,google_street_view_link,sms_phone,sms_opt_out,netsuite_id,lead_source,lead_price_paid,country,duplicate_result_from_netsuite,home_type,preferred_method_of_follow_up,lead_generator_comment,lead_generator_date,lead_generator_time,updates_about_sunedison,how_much_do_you_want_to_cut_your_bill,homeowner_require_financing,water_heater_type,interested_in,originator,originator_email,sunedison_employee_referrer_firstname,sunedison_employee_referrer_lastname,sunedison_employee_referrer_email,does_your_house_have_an_attic,do_you_have_a_central_heating_and_cooling_system,primary_phone,credit_score,custom_1,adressee,total_system_size_echowatts,total_retail_price_quoted,contract_cancelled_date,interested_in_purchase_type,financing_program,solar_designer,lost_reason,lost,lost_cancel_comments,homeowner_lease_contract_status,homeowner_credit_check_status,address_2,proposal_status,campaign_subcategory,name,address,google_street_view_link1,email1,primary_phone1,home_phone1,mobile_phone1,work_phone1,international_phone,netsuite_token_id,advisor_notes,solar_advisor,kw_sold,system_price,energy_consultant,energy_consultant_notes,other_notes,lead_source_manual,designer_notes,pricing_exceptions,id,collected_utility_bill,contract_status,createdate,modifydate,last_distribution_date,lead_title,status,agent_name,agent_email,campaign):
	leadFieldsVariables = "|" + firstname + "|,|" + lastname + "|,|" + home_address + "|,|" + city + "|,|" + state + "|,|" + zip + "|,|" + home_phone + "|,|" + work_phone + "|,|" + email + "|,|" +  average_monthly_electric_bill + "|,|" + electricity_utility_company + "|,|" + do_you_own_the_home + "|,|" + roof_shade + "|,|" + roof_type + "|,|" + google_street_view_link + "|,|" + sms_phone + "|,|" + sms_opt_out + "|,|" + netsuite_id + "|,|" + lead_source + "|,|" + lead_price_paid + "|,|" + country + "|,|" + duplicate_result_from_netsuite + "|,|" + home_type + "|,|" + preferred_method_of_follow_up + "|,|" + lead_generator_comment + "|,|" + lead_generator_date + "|,|" + lead_generator_time + "|,|" + updates_about_sunedison + "|,|" + how_much_do_you_want_to_cut_your_bill + "|,|" + homeowner_require_financing + "|,|" + water_heater_type + "|,|" + interested_in + "|,|" + originator + "|,|" + originator_email + "|,|" + sunedison_employee_referrer_firstname + "|,|" + sunedison_employee_referrer_lastname + "|,|" + sunedison_employee_referrer_email + "|,|" + does_your_house_have_an_attic + "|,|" + do_you_have_a_central_heating_and_cooling_system + "|,|" + primary_phone + "|,|" + credit_score + "|,|" + custom_1 + "|,|" + adressee + "|,|" + total_system_size_echowatts + "|,|" + total_retail_price_quoted + "|,|" + contract_cancelled_date + "|,|" + interested_in_purchase_type + "|,|" + financing_program + "|,|" + solar_designer + "|,|" + lost_reason + "|,|" + lost + "|,|" + lost_cancel_comments + "|,|" + homeowner_lease_contract_status + "|,|" + homeowner_credit_check_status + "|,|" + address_2 + "|,|" + proposal_status + "|,|" + campaign_subcategory + "|,|" + name + "|,|" + address + "|,|" + google_street_view_link1 + "|,|" + email1 + "|,|" + primary_phone1 + "|,|" + home_phone1 + "|,|" + mobile_phone1 + "|,|" + work_phone1 + "|,|" + international_phone + "|,|" + netsuite_token_id + "|,|" + advisor_notes + "|,|" + solar_advisor + "|,|" + kw_sold + "|,|" + system_price + "|,|" + energy_consultant  + "|,|" + energy_consultant_notes + "|,|" + other_notes  + "|,|" + lead_source_manual  + "|,|" + designer_notes  + "|,|" + pricing_exceptions  + "|,|" + id  + "|,|" + collected_utility_bill + "|,|" + contract_status  + "|,|" + createdate  + "|,|" + modifydate  + "|,|" + last_distribution_date  + "|,|" + lead_title  + "|,|" + status  + "|,|" + agent_name  + "|,|" + agent_email  + "|,|" + campaign + "|\n";
	leadFieldsCSV.write(leadFieldsVariables);
	print("Data written for ID(Leads): %s" %id);

def generateStatusLogCsv(AgentName,GroupName,StatusTitle,AgentEmail,LogDate,id,CreateDate,ModifyDate):
	statusLogVariables = "^" + AgentName + "^,^" + GroupName + "^,^" + StatusTitle + "^,^" + AgentEmail + "^,^" + LogDate + "^,^" + id + "^,^" + CreateDate + "^,^" + ModifyDate + "^\n";
	statusLogCSV.write(statusLogVariables);
	print("Data written for ID(StatusLog): %s" %id);

#def connectToDatabase():
#	conn_string = "dbname='sunedison' port='5439' user='abhardwaj' password='Master12' host='sunedisondatawarehouse.cgnr3c8sn1sz.us-west-2.redshift.amazonaws.com'";
#	print "Connecting to database";
#	conn = psycopg2.connect(conn_string);
#	print "Connected to DB";
#	cursor = conn.cursor();
#
#	cursor.execute("select * from customer");
#	rows = cursor.fetchall();
#
#	for row in rows: 
#		print(row);
#
#	conn.commit();
#	conn.close();


getLeadIds();
