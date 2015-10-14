package com.sunedison;

import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.Properties;

import javax.net.ssl.SSLEngineResult.Status;

import org.json.*;

public class VelocifyDataToCSV {
	public static void main(String args[]){
		VelocifyDataToCSV obj = new VelocifyDataToCSV();
		try{
			FileWriter writer = new FileWriter("//Users//anuragbhardwaj//Desktop//SunEdison//LeadsData//leads.csv");
			FileWriter creationLogCSV = new FileWriter("//Users//anuragbhardwaj//Desktop//SunEdison//LeadsData//creationLogCSV.csv");
			FileWriter statusLogCSV = new FileWriter("//Users//anuragbhardwaj//Desktop//SunEdison//LeadsData//statusLogCSV.csv");
			FileWriter exportLogCSV = new FileWriter("//Users//anuragbhardwaj//Desktop//SunEdison//LeadsData//exportLogCSV.csv");
			FileWriter distributionLogCSV = new FileWriter("//Users//anuragbhardwaj//Desktop//SunEdison//LeadsData//distributionLogCSV.csv");
			FileWriter actionLogCSV = new FileWriter("//Users//anuragbhardwaj//Desktop//SunEdison//LeadsData//actionLogCSV.csv");
			FileWriter assignmentLogCSV = new FileWriter("//Users//anuragbhardwaj//Desktop//SunEdison//LeadsData//assignmentLogCSV.csv");
			FileWriter emailLogCSV = new FileWriter("//Users//anuragbhardwaj//Desktop//SunEdison//LeadsData//emailLogCSV.csv");
			obj.getLead(writer,creationLogCSV,statusLogCSV,exportLogCSV,distributionLogCSV,actionLogCSV,assignmentLogCSV,emailLogCSV);
		}
		catch(IOException e){
			System.out.println(e);
		}
	}	
	
	public void getLead(FileWriter writer,FileWriter creationLogCSV,FileWriter statusLogCSV,FileWriter exportLogCSV,FileWriter distributionLogCSV,FileWriter actionLogCSV,FileWriter assignmentLogCSV,FileWriter emailLogCSV){
		URL url;
		HttpURLConnection conn;
	    BufferedReader rd;
	    String line;
	    StringBuilder result = new StringBuilder();
	    try {
	         url = new URL("https://service.leads360.com/ClientService.asmx/GetLeadIds?username=abhardwaj@sunedison.com&password=F9nuvdgetu4?&from=01/01/2000&to=10/14/2015");
	         conn = (HttpURLConnection) url.openConnection();
	         conn.setRequestMethod("GET");
	         rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
	         while ((line = rd.readLine()) != null) {
	            result.append(line);
	         }
	         JSONObject xmlJSONObj = XML.toJSONObject(result.toString());
	         //System.out.println(xmlJSONObj); 
	         for(int i=0;i<xmlJSONObj.getJSONObject("Leads").getJSONArray("Lead").length();i++){
	        	 JSONObject Lead = xmlJSONObj.getJSONObject("Leads").getJSONArray("Lead").getJSONObject(i);
	        	 //System.out.println(Lead);
	        	 int id = Lead.getInt("Id");
	        	 getLeadInfo(id, writer,creationLogCSV,statusLogCSV,exportLogCSV,distributionLogCSV,actionLogCSV,assignmentLogCSV,emailLogCSV);
	        	 //System.out.println(id);
	         }
	         
	         rd.close();
	    } catch (IOException e) {
	         e.printStackTrace();
	    } catch (Exception e) {
	         e.printStackTrace();
	    }
	}
	
	public void getLeadInfo(int id, final FileWriter writer,final FileWriter creationLogCSV,final FileWriter statusLogCSV,final FileWriter exportLogCSV,final FileWriter distributionLogCSV,final FileWriter actionLogCSV,final FileWriter assignmentLogCSV,final FileWriter emailLogCSV){
		URL url;
		HttpURLConnection conn;
	    BufferedReader rd;
	    String line;
	    StringBuilder result = new StringBuilder();
	    try {
	         url = new URL("https://service.leads360.com/ClientService.asmx/GetLead?username=abhardwaj@sunedison.com&password=F9nuvdgetu4?&leadId="+id);
	         conn = (HttpURLConnection) url.openConnection();
	         conn.setRequestMethod("GET");
	         rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
	         while ((line = rd.readLine()) != null) {
	            result.append(line);
	         }
	         final JSONObject xmlJSONObj = XML.toJSONObject(result.toString());
	         //System.out.println(xmlJSONObj);
	         
	         
	         Thread thread8 = new Thread(){
	        	 public void run(){
	        		 checkForExistence(xmlJSONObj,writer);
	        	 }
	         };
	         
	         /* Thread thread1 = new Thread(){
	        	 public void run(){
	        		 try {
						getCreationLog(xmlJSONObj,creationLogCSV);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	        	 }
	         };
	         
	         Thread thread2 = new Thread(){
	        	public void run(){
	        		try {
						getStatusLog(xmlJSONObj,statusLogCSV);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	        	}
	         };
	         
	         Thread thread3 = new Thread(){
	        	public void run(){
	        		try {
	        			getExportLog(xmlJSONObj,exportLogCSV);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	        	}
	         };
	         
	         Thread thread4 = new Thread(){
	        	public void run(){
	        		try {
	        			getDistributionLog(xmlJSONObj,distributionLogCSV);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	        	}
	         }; */
	         
	         Thread thread5 = new Thread(){
	        	public void run(){
	        		try {
	        			getActionLog(xmlJSONObj,actionLogCSV);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	        	}
	         };
	         
	      /*   Thread thread6 = new Thread(){
	        	public void run(){
	        		try {
	        			getAssignmentLog(xmlJSONObj,assignmentLogCSV);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	        	}
	         };
	         
	         Thread thread7 = new Thread(){
	        	public void run(){
	        		try {
	        			getEmailLog(xmlJSONObj,emailLogCSV);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	        	}
	         }; */
	         
	         //thread1.start();
	         //thread2.start();
	         //thread3.start();
	         //thread4.start();
	         thread5.start();
	         //thread6.start();
	         //thread7.start();
	         thread8.start();
	         
	         //thread1.join();
	         //thread2.join();
	         //thread3.join();
	         //thread4.join();
	         thread5.join();
	         //thread6.join();
	         //thread7.join();
	         thread8.join();
	         
	         
	         
	         rd.close();
	    } catch (IOException e) {
	         e.printStackTrace();
	    } catch (Exception e) {
	         e.printStackTrace();
	    }
	}
	
	public void getCreationLog(JSONObject xmlJSONObj, FileWriter creationLogCSV) throws IOException{ 
		String AssignedAgentName = ""; //Exisiting Check
		String CreatedByAgentName = "";
		String CreatedByAgentEmail = "";
		String AssignedGroupName = "";
		String AssignedAgentEmail = "";
		String CreatedByGroupName = "";
		String LogDate = "";
		String VelocifyLeadId = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("Id").toString();
		if(xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").getJSONObject("Logs").has("CreationLog")){
			JSONObject creationLog = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").getJSONObject("Logs").getJSONObject("CreationLog");
			if(creationLog.has("AssignedAgentName"))
				AssignedAgentName = creationLog.get("AssignedAgentName").toString();
			if(creationLog.has("CreatedByAgentName"))
				CreatedByAgentName = creationLog.get("CreatedByAgentName").toString();
			if(creationLog.has("CreatedByAgentEmail"))
				CreatedByAgentEmail = creationLog.get("CreatedByAgentEmail").toString();
			if(creationLog.has("AssignedGroupName"))
				AssignedGroupName = creationLog.get("AssignedGroupName").toString();
			if(creationLog.has("AssignedAgentEmail"))
				AssignedAgentEmail = creationLog.get("AssignedAgentEmail").toString();
			if(creationLog.has("CreatedByGroupName"))
				CreatedByGroupName = creationLog.get("CreatedByGroupName").toString();
			
			String createdate = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("CreateDate").toString();
			String modifydate = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("ModifyDate").toString();
			
			generateCreationLogCSV(creationLogCSV,AssignedAgentName,CreatedByAgentName,CreatedByAgentEmail,AssignedGroupName,AssignedAgentEmail,CreatedByGroupName,LogDate,VelocifyLeadId,createdate,modifydate);
		}
	}
	
	public void generateCreationLogCSV(FileWriter creationLogCSV,String AssignedAgentName,String CreatedByAgentName,String CreatedByAgentEmail,String AssignedGroupName,String AssignedAgentEmail,String CreatedByGroupName,String LogDate,String VelocifyLeadId, String createdate, String modifydate) throws IOException{
		creationLogCSV.append("^"+AssignedAgentName+"^");
		creationLogCSV.append(',');
		creationLogCSV.append("^"+CreatedByAgentName+"^");
		creationLogCSV.append(',');
		creationLogCSV.append("^"+CreatedByAgentEmail+"^");
		creationLogCSV.append(',');
		creationLogCSV.append("^"+AssignedGroupName+"^");
		creationLogCSV.append(',');
		creationLogCSV.append("^"+AssignedAgentEmail+"^");
		creationLogCSV.append(',');
		creationLogCSV.append("^"+CreatedByGroupName+"^");
		creationLogCSV.append(',');
		creationLogCSV.append("^"+LogDate+"^");
		creationLogCSV.append(',');
		creationLogCSV.append("^"+VelocifyLeadId+"^");
		creationLogCSV.append(',');
		creationLogCSV.append("^"+createdate+"^");
		creationLogCSV.append(',');
		creationLogCSV.append("^"+modifydate+"^");
		creationLogCSV.append("\n");
		System.out.println("Record written for ID for Creation Log: " + VelocifyLeadId);
		creationLogCSV.flush();
	}
	
	public void getStatusLog(JSONObject xmlJSONObj, FileWriter statusLogCSV) throws IOException{
		String AgentName = "";
		String GroupName = ""; //exisiting check
		String StatusTitle = "";
		String AgentEmail = "";
		String LogDate = "";
		String VelocifyLeadId = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("Id").toString();
		if(xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").getJSONObject("Logs").has("StatusLog")){
			Object statusLog = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").getJSONObject("Logs").getJSONObject("StatusLog").get("Status");
			if(statusLog instanceof JSONArray){
				for(int i=0;i<xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").getJSONObject("Logs").getJSONObject("StatusLog").getJSONArray("Status").length();i++){
					if(((JSONArray) statusLog).getJSONObject(i).has("AgentName"))
						AgentName = ((JSONArray) statusLog).getJSONObject(i).get("AgentName").toString();
					if(((JSONArray) statusLog).getJSONObject(i).has("GroupName"))
						GroupName = ((JSONArray) statusLog).getJSONObject(i).get("GroupName").toString();
					if(((JSONArray) statusLog).getJSONObject(i).has("StatusTitle"))
						StatusTitle = ((JSONArray) statusLog).getJSONObject(i).get("StatusTitle").toString();
					if(((JSONArray) statusLog).getJSONObject(i).has("AgentEmail"))
						AgentEmail = ((JSONArray) statusLog).getJSONObject(i).get("AgentEmail").toString();
					if(((JSONArray) statusLog).getJSONObject(i).has("LogDate"))
						LogDate = ((JSONArray) statusLog).getJSONObject(i).get("LogDate").toString();
					
					String createdate = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("CreateDate").toString();
					String modifydate = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("ModifyDate").toString();
					
					generateStatusLogCSV(statusLogCSV,AgentName,GroupName,StatusTitle,AgentEmail,LogDate,VelocifyLeadId,createdate,modifydate);
				}
			}
			else{
				if(((JSONObject) statusLog).has("AgentName"))
					AgentName = ((JSONObject) statusLog).get("AgentName").toString();
				if(((JSONObject) statusLog).has("GroupName"))
					GroupName = ((JSONObject) statusLog).get("GroupName").toString();
				if(((JSONObject) statusLog).has("StatusTitle"))
					StatusTitle = ((JSONObject) statusLog).get("StatusTitle").toString();
				if(((JSONObject) statusLog).has("AgentEmail"))
					AgentEmail = ((JSONObject) statusLog).get("AgentEmail").toString();
				if(((JSONObject) statusLog).has("LogDate"))
					LogDate = ((JSONObject) statusLog).get("LogDate").toString();
				
				String createdate = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("CreateDate").toString();
				String modifydate = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("ModifyDate").toString();
				
				generateStatusLogCSV(statusLogCSV,AgentName,GroupName,StatusTitle,AgentEmail,LogDate,VelocifyLeadId,createdate,modifydate);
			}
			
		}
	}
	
	public void generateStatusLogCSV(FileWriter statusLogCSV,String AgentName,String GroupName,String StatusTitle,String AgentEmail,String LogDate,String VelocifyLeadId,String createdate, String modifydate) throws IOException{
		statusLogCSV.append("^"+AgentName+"^");
		statusLogCSV.append(',');
		statusLogCSV.append("^"+GroupName+"^");
		statusLogCSV.append(',');
		statusLogCSV.append("^"+StatusTitle+"^");
		statusLogCSV.append(',');
		statusLogCSV.append("^"+AgentEmail+"^");
		statusLogCSV.append(',');
		statusLogCSV.append("^"+LogDate+"^");
		statusLogCSV.append(',');
		statusLogCSV.append("^"+VelocifyLeadId+"^");
		statusLogCSV.append(',');
		statusLogCSV.append("^"+createdate+"^");
		statusLogCSV.append(',');
		statusLogCSV.append("^"+modifydate+"^");
		statusLogCSV.append("\n");
		System.out.println("Record written for ID for Status Log: " + VelocifyLeadId);
		statusLogCSV.flush();
	}
	
	public void getExportLog(JSONObject xmlJSONObj, FileWriter exportLogCSV) throws IOException{
		String AgentName = "";
		String Result = "";
		String Message = "";
		String System = "";
		String AgentEmail = "";
		String LogDate = "";
		String VelocifyLeadId = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("Id").toString();
		if(xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").getJSONObject("Logs").has("ExportLog")){
			Object exportLog = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").getJSONObject("Logs").getJSONObject("ExportLog").get("Export");
			if(exportLog instanceof JSONArray){
				for(int i=0;i<xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").getJSONObject("Logs").getJSONObject("ExportLog").getJSONArray("Export").length();i++){
					if(((JSONArray) exportLog).getJSONObject(i).has("AgentName"))
						AgentName = ((JSONArray) exportLog).getJSONObject(i).get("AgentName").toString();
					if(((JSONArray) exportLog).getJSONObject(i).has("Result"))
						Result = ((JSONArray) exportLog).getJSONObject(i).get("Result").toString();
					if(((JSONArray) exportLog).getJSONObject(i).has("Message"))
						Message = ((JSONArray) exportLog).getJSONObject(i).get("Message").toString();
					if(((JSONArray) exportLog).getJSONObject(i).has("System"))
						System = ((JSONArray) exportLog).getJSONObject(i).get("System").toString();
					if(((JSONArray) exportLog).getJSONObject(i).has("AgentEmail"))
						AgentEmail = ((JSONArray) exportLog).getJSONObject(i).get("AgentEmail").toString();
					if(((JSONArray) exportLog).getJSONObject(i).has("LogDate"))
						LogDate = ((JSONArray) exportLog).getJSONObject(i).get("LogDate").toString();
					
					String createdate = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("CreateDate").toString();
					String modifydate = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("ModifyDate").toString();
					
					generateExportLogCSV(exportLogCSV,AgentName,Result,Message,System,AgentEmail,LogDate,VelocifyLeadId,createdate,modifydate);
				}
			}
			else{
				if(((JSONObject) exportLog).has("AgentName"))
					AgentName = ((JSONObject) exportLog).get("AgentName").toString();
				if(((JSONObject) exportLog).has("Result"))
					Result = ((JSONObject) exportLog).get("Result").toString();
				if(((JSONObject) exportLog).has("Message"))
					Message = ((JSONObject) exportLog).get("Message").toString();
				if(((JSONObject) exportLog).has("System"))
					System = ((JSONObject) exportLog).get("System").toString();
				if(((JSONObject) exportLog).has("AgentEmail"))
					AgentEmail = ((JSONObject) exportLog).get("AgentEmail").toString();
				if(((JSONObject) exportLog).has("LogDate"))
					LogDate = ((JSONObject) exportLog).get("LogDate").toString();
				
				String createdate = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("CreateDate").toString();
				String modifydate = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("ModifyDate").toString();
				
				generateExportLogCSV(exportLogCSV,AgentName,Result,Message,System,AgentEmail,LogDate,VelocifyLeadId,createdate,modifydate);
			}
			
			
		}
	}
	
	public void generateExportLogCSV(FileWriter exportLogCSV,String AgentName,String Result,String Message,String System1,String AgentEmail,String LogDate,String VelocifyLeadId, String createdate, String modifydate) throws IOException{
		exportLogCSV.append("^"+AgentName+"^");
		exportLogCSV.append(',');
		exportLogCSV.append("^"+Result+"^");
		exportLogCSV.append(',');
		exportLogCSV.append("^"+Message+"^");
		exportLogCSV.append(',');
		exportLogCSV.append("^"+System1+"^");
		exportLogCSV.append(',');
		exportLogCSV.append("^"+AgentEmail+"^");
		exportLogCSV.append(',');
		exportLogCSV.append("^"+LogDate+"^");
		exportLogCSV.append(',');
		exportLogCSV.append("^"+VelocifyLeadId+"^");
		exportLogCSV.append(',');
		exportLogCSV.append("^"+createdate+"^");
		exportLogCSV.append(',');
		exportLogCSV.append("^"+modifydate+"^");
		exportLogCSV.append("\n");
		System.out.println("Record written for ID for Export Log: " + VelocifyLeadId);
		exportLogCSV.flush();
	}
	
	public void getDistributionLog(JSONObject xmlJSONObj, FileWriter distributionLogCSV) throws IOException{
		String AssignedAgentName = "";
		String AssignedGroupName = "";
		String AssignedAgentEmail = "";
		String DistributionProgramName = "";
		String LogDate = "";
		String VelocifyLeadId = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("Id").toString();
		if(xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").getJSONObject("Logs").has("DistributionLog")){
			Object distributionLog = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").getJSONObject("Logs").getJSONObject("DistributionLog").get("Distribution");
			if(distributionLog instanceof JSONArray){
				for(int i=0;i<xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").getJSONObject("Logs").getJSONObject("DistributionLog").getJSONArray("Distribution").length();i++){
					if(((JSONArray) distributionLog).getJSONObject(i).has("AssignedAgentName"))
						AssignedAgentName = ((JSONArray) distributionLog).getJSONObject(i).get("AssignedAgentName").toString();
					if(((JSONArray) distributionLog).getJSONObject(i).has("AssignedGroupName"))
						AssignedGroupName = ((JSONArray) distributionLog).getJSONObject(i).get("AssignedGroupName").toString();
					if(((JSONArray) distributionLog).getJSONObject(i).has("AssignedAgentEmail"))
						AssignedAgentEmail = ((JSONArray) distributionLog).getJSONObject(i).get("AssignedAgentEmail").toString();
					if(((JSONArray) distributionLog).getJSONObject(i).has("DistributionProgramName"))
						DistributionProgramName = ((JSONArray) distributionLog).getJSONObject(i).get("DistributionProgramName").toString();
					if(((JSONArray) distributionLog).getJSONObject(i).has("LogDate"))
						LogDate = ((JSONArray) distributionLog).getJSONObject(i).get("LogDate").toString();
					
					String createdate = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("CreateDate").toString();
					String modifydate = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("ModifyDate").toString();
					
					generateDistributionLogCSV(distributionLogCSV,AssignedAgentName,AssignedGroupName,AssignedAgentEmail,DistributionProgramName,LogDate,VelocifyLeadId,createdate,modifydate);
				}
			}
			else{
				if(((JSONObject) distributionLog).has("AssignedAgentName"))
					AssignedAgentName = ((JSONObject) distributionLog).get("AssignedAgentName").toString();
				if(((JSONObject) distributionLog).has("AssignedGroupName"))
					AssignedGroupName = ((JSONObject) distributionLog).get("AssignedGroupName").toString();
				if(((JSONObject) distributionLog).has("AssignedAgentEmail"))
					AssignedAgentEmail = ((JSONObject) distributionLog).get("AssignedAgentEmail").toString();
				if(((JSONObject) distributionLog).has("DistributionProgramName"))
					DistributionProgramName = ((JSONObject) distributionLog).get("DistributionProgramName").toString();
				if(((JSONObject) distributionLog).has("LogDate"))
					LogDate = ((JSONObject) distributionLog).get("LogDate").toString();
				
				String createdate = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("CreateDate").toString();
				String modifydate = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("ModifyDate").toString();
				
				generateDistributionLogCSV(distributionLogCSV,AssignedAgentName,AssignedGroupName,AssignedAgentEmail,DistributionProgramName,LogDate,VelocifyLeadId,createdate,modifydate);
			}
			
			
		}
	}
	
	public void generateDistributionLogCSV(FileWriter distributionLogCSV,String AssignedAgentName,String AssignedGroupName,String AssignedAgentEmail,String DistributionProgramName,String LogDate,String VelocifyLeadId,String createdate, String modifydate) throws IOException{
		distributionLogCSV.append("^"+AssignedAgentName+"^");
		distributionLogCSV.append(',');
		distributionLogCSV.append("^"+AssignedGroupName+"^");
		distributionLogCSV.append(',');
		distributionLogCSV.append("^"+AssignedAgentEmail+"^");
		distributionLogCSV.append(',');
		distributionLogCSV.append("^"+DistributionProgramName+"^");
		distributionLogCSV.append(',');
	    distributionLogCSV.append("^"+LogDate+"^");
		distributionLogCSV.append(',');
		distributionLogCSV.append("^"+VelocifyLeadId+"^");
		distributionLogCSV.append(',');
		distributionLogCSV.append("^"+createdate+"^");
		distributionLogCSV.append(',');
		distributionLogCSV.append("^"+modifydate+"^");
		distributionLogCSV.append("\n");
		System.out.println("Record written for ID for Distribution Log: " + VelocifyLeadId);
		distributionLogCSV.flush();
	}
	
	public void getActionLog(JSONObject xmlJSONObj, FileWriter actionLogCSV) throws IOException{
		String AgentName = "";
		String ActionTypeName = "";
		String ActionNote = "";
		String GroupName = "";
		String AgentEmail = "";
		String ActionDate = "";
		String VelocifyLeadId = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("Id").toString();
		if(xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").getJSONObject("Logs").has("ActionLog")){
			Object actionLog = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").getJSONObject("Logs").getJSONObject("ActionLog").get("Action");
			if(actionLog instanceof JSONArray){
				for(int i=0;i<xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").getJSONObject("Logs").getJSONObject("ActionLog").getJSONArray("Action").length();i++){
					if(((JSONArray) actionLog).getJSONObject(i).has("AgentName"))
						AgentName = ((JSONArray) actionLog).getJSONObject(i).get("AgentName").toString();
					
					if(((JSONArray) actionLog).getJSONObject(i).has("ActionTypeName"))
						ActionTypeName = ((JSONArray) actionLog).getJSONObject(i).get("ActionTypeName").toString();
					
					if(((JSONArray) actionLog).getJSONObject(i).has("ActionNote"))
						ActionNote = ((JSONArray) actionLog).getJSONObject(i).get("ActionNote").toString();
					
					if(((JSONArray) actionLog).getJSONObject(i).has("GroupName"))
						GroupName = ((JSONArray) actionLog).getJSONObject(i).get("GroupName").toString();
					
					if(((JSONArray) actionLog).getJSONObject(i).has("AgentEmail"))
						AgentEmail = ((JSONArray) actionLog).getJSONObject(i).get("AgentEmail").toString();
					
					if(((JSONArray) actionLog).getJSONObject(i).has("ActionDate"))
						ActionDate = ((JSONArray) actionLog).getJSONObject(i).get("ActionDate").toString();
					
					String createdate = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("CreateDate").toString();
					String modifydate = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("ModifyDate").toString();
					
					generateActionLogCSV(actionLogCSV,AgentName,ActionTypeName,ActionNote,GroupName,AgentEmail,ActionDate,VelocifyLeadId,createdate,modifydate);
				}
			}
			else{
				if(((JSONObject) actionLog).has("AgentName"))
					AgentName = ((JSONObject) actionLog).get("AgentName").toString();
				if(((JSONObject) actionLog).has("ActionTypeName"))
					ActionTypeName = ((JSONObject) actionLog).get("ActionTypeName").toString();
				if(((JSONObject) actionLog).has("ActionNote"))
					ActionNote = ((JSONObject) actionLog).get("ActionNote").toString();
				if(((JSONObject) actionLog).has("GroupName"))
					GroupName = ((JSONObject) actionLog).get("GroupName").toString();
				if(((JSONObject) actionLog).has("AgentEmail"))
					AgentEmail = ((JSONObject) actionLog).get("AgentEmail").toString();
				if(((JSONObject) actionLog).has("ActionDate"))
					ActionDate = ((JSONObject) actionLog).get("ActionDate").toString();
				
				String createdate = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("CreateDate").toString();
				String modifydate = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("ModifyDate").toString();
				
				generateActionLogCSV(actionLogCSV,AgentName,ActionTypeName,ActionNote,GroupName,AgentEmail,ActionDate,VelocifyLeadId,createdate,modifydate);
			}	
		}
	}
	
	public void generateActionLogCSV(FileWriter actionLogCSV,String AgentName,String ActionTypeName,String ActionNote,String GroupName,String AgentEmail,String ActionDate,String VelocifyLeadId,String createdate, String modifydate) throws IOException{
		actionLogCSV.append("^"+AgentName+"^");
		actionLogCSV.append(',');
		actionLogCSV.append("^"+ActionTypeName+"^");
		actionLogCSV.append(',');
		actionLogCSV.append("^"+ActionNote+"^");
		actionLogCSV.append(',');
		actionLogCSV.append("^"+GroupName+"^");
		actionLogCSV.append(',');
	    actionLogCSV.append("^"+AgentEmail+"^");
		actionLogCSV.append(',');
		actionLogCSV.append("^"+ActionDate+"^");
		actionLogCSV.append(',');
		actionLogCSV.append("^"+VelocifyLeadId+"^");
		actionLogCSV.append(',');
		actionLogCSV.append("^"+createdate+"^");
		actionLogCSV.append(',');
		actionLogCSV.append("^"+modifydate+"^");
		actionLogCSV.append("\n");
		System.out.println("Record written for ID for Action Log: " + VelocifyLeadId);
		actionLogCSV.flush();
	}
	
	public void getAssignmentLog(JSONObject xmlJSONObj, FileWriter assignmentLogCSV) throws IOException{
		String AssignedAgentName = "";
		String AssignedByAgentName = "";
		String AssignedByGroupName = "";
		String AssignedGroupName = "";
		String AssignedByAgentEmail = "";
		String AssignedAgentEmail = "";
		String LogDate = "";
		String VelocifyLeadId = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("Id").toString();
		if(xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").getJSONObject("Logs").has("AssignmentLog")){
			Object assignmentLog = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").getJSONObject("Logs").getJSONObject("AssignmentLog").get("Assignment");
			if(assignmentLog instanceof JSONArray){
				for(int i=0;i<xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").getJSONObject("Logs").getJSONObject("AssignmentLog").getJSONArray("Assignment").length();i++){
					if(((JSONArray) assignmentLog).getJSONObject(i).has("AssignedAgentName"))
						AssignedAgentName = ((JSONArray) assignmentLog).getJSONObject(i).get("AssignedAgentName").toString();
					if(((JSONArray) assignmentLog).getJSONObject(i).has("AssignedByAgentName"))
						AssignedByAgentName = ((JSONArray) assignmentLog).getJSONObject(i).get("AssignedByAgentName").toString();
					if(((JSONArray) assignmentLog).getJSONObject(i).has("AssignedByGroupName"))
						AssignedByGroupName = ((JSONArray) assignmentLog).getJSONObject(i).get("AssignedByGroupName").toString();
					if(((JSONArray) assignmentLog).getJSONObject(i).has("AssignedGroupName"))
						AssignedGroupName = ((JSONArray) assignmentLog).getJSONObject(i).get("AssignedGroupName").toString();
					if(((JSONArray) assignmentLog).getJSONObject(i).has("AssignedByAgentEmail"))
						AssignedByAgentEmail = ((JSONArray) assignmentLog).getJSONObject(i).get("AssignedByAgentEmail").toString();
					if(((JSONArray) assignmentLog).getJSONObject(i).has("AssignedAgentEmail"))
						AssignedAgentEmail = ((JSONArray) assignmentLog).getJSONObject(i).get("AssignedAgentEmail").toString();
					if(((JSONArray) assignmentLog).getJSONObject(i).has("LogDate"))
						LogDate = ((JSONArray) assignmentLog).getJSONObject(i).get("LogDate").toString();
					
					String createdate = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("CreateDate").toString();
					String modifydate = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("ModifyDate").toString();
					
					generateAssignmentCSV(assignmentLogCSV,AssignedAgentName,AssignedByAgentName,AssignedByGroupName,AssignedGroupName,AssignedByAgentEmail,AssignedAgentEmail,LogDate,VelocifyLeadId,createdate,modifydate);
				}
			}
			else{
				if(((JSONObject) assignmentLog).has("AssignedAgentName"))
					AssignedAgentName = ((JSONObject) assignmentLog).get("AssignedAgentName").toString();
				if(((JSONObject) assignmentLog).has("AssignedByAgentName"))
					AssignedByAgentName = ((JSONObject) assignmentLog).get("AssignedByAgentName").toString();
				if(((JSONObject) assignmentLog).has("AssignedByGroupName"))
					AssignedByGroupName = ((JSONObject) assignmentLog).get("AssignedByGroupName").toString();
				if(((JSONObject) assignmentLog).has("AssignedGroupName"))
					AssignedGroupName = ((JSONObject) assignmentLog).get("AssignedGroupName").toString();
				if(((JSONObject) assignmentLog).has("AssignedByAgentEmail"))
					AssignedByAgentEmail = ((JSONObject) assignmentLog).get("AssignedByAgentEmail").toString();
				if(((JSONObject) assignmentLog).has("AssignedAgentEmail"))
					AssignedAgentEmail = ((JSONObject) assignmentLog).get("AssignedAgentEmail").toString();
				if(((JSONObject) assignmentLog).has("LogDate"))
					LogDate = ((JSONObject) assignmentLog).get("LogDate").toString();
				
				String createdate = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("CreateDate").toString();
				String modifydate = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("ModifyDate").toString();
				
				generateAssignmentCSV(assignmentLogCSV,AssignedAgentName,AssignedByAgentName,AssignedByGroupName,AssignedGroupName,AssignedByAgentEmail,AssignedAgentEmail,LogDate,VelocifyLeadId,createdate,modifydate);
			}	
		}
	}
	
	public void generateAssignmentCSV(FileWriter assignmentLogCSV,String AssignedAgentName,String AssignedByAgentName,String AssignedByGroupName,String AssignedGroupName,String AssignedByAgentEmail,String AssignedAgentEmail,String LogDate,String VelocifyLeadId,String createdate, String modifydate) throws IOException{
		assignmentLogCSV.append("^"+AssignedAgentName+"^");
		assignmentLogCSV.append(',');
		assignmentLogCSV.append("^"+AssignedByAgentName+"^");
		assignmentLogCSV.append(',');
		assignmentLogCSV.append("^"+AssignedByGroupName+"^");
		assignmentLogCSV.append(',');
		assignmentLogCSV.append("^"+AssignedGroupName+"^");
		assignmentLogCSV.append(',');
	    assignmentLogCSV.append("^"+AssignedByAgentEmail+"^");
		assignmentLogCSV.append(',');
		assignmentLogCSV.append("^"+AssignedAgentEmail+"^");
		assignmentLogCSV.append(',');
		assignmentLogCSV.append("^"+LogDate+"^");
		assignmentLogCSV.append(',');
		assignmentLogCSV.append("^"+VelocifyLeadId+"^");
		assignmentLogCSV.append(',');
		assignmentLogCSV.append("^"+createdate+"^");
		assignmentLogCSV.append(',');
		assignmentLogCSV.append("^"+modifydate+"^");
		assignmentLogCSV.append("\n");
		System.out.println("Record written for ID for Assignment Log: " + VelocifyLeadId);
		assignmentLogCSV.flush();
	}
	
	public void getEmailLog(JSONObject xmlJSONObj, FileWriter emailLogCSV) throws IOException{
		String AgentName = "";
		String SendDate = "";
		String EmailTemplateName = "";
		String VelocifyLeadId = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("Id").toString();
		if(xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").getJSONObject("Logs").has("EmailLog")){
			Object emailLog = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").getJSONObject("Logs").getJSONObject("EmailLog").get("Email");
			if(emailLog instanceof JSONArray){
				for(int i=0;i<xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").getJSONObject("Logs").getJSONObject("EmailLog").getJSONArray("Email").length();i++){
					if(((JSONArray) emailLog).getJSONObject(i).has("AgentName"))
						AgentName = ((JSONArray) emailLog).getJSONObject(i).get("AgentName").toString();
					if(((JSONArray) emailLog).getJSONObject(i).has("SendDate"))
						SendDate = ((JSONArray) emailLog).getJSONObject(i).get("SendDate").toString();
					if(((JSONArray) emailLog).getJSONObject(i).has("EmailTemplateName"))
						EmailTemplateName = ((JSONArray) emailLog).getJSONObject(i).get("EmailTemplateName").toString();
					
					String createdate = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("CreateDate").toString();
					String modifydate = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("ModifyDate").toString();
					
					generateEmailLogCSV(emailLogCSV,AgentName,SendDate,EmailTemplateName,VelocifyLeadId,createdate,modifydate);
				}
			}
			else{
				if(((JSONObject) emailLog).has("AgentName"))
					AgentName = ((JSONObject) emailLog).get("AgentName").toString();
				if(((JSONObject) emailLog).has("SendDate"))
					SendDate = ((JSONObject) emailLog).get("SendDate").toString();
				if(((JSONObject) emailLog).has("EmailTemplateName"))
					EmailTemplateName = ((JSONObject) emailLog).get("EmailTemplateName").toString();
				
				String createdate = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("CreateDate").toString();
				String modifydate = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("ModifyDate").toString();
				
				generateEmailLogCSV(emailLogCSV,AgentName,SendDate,EmailTemplateName,VelocifyLeadId,createdate,modifydate);
			}	
		}
	}
	
	public void generateEmailLogCSV(FileWriter emailLogCSV,String AgentName,String SendDate,String EmailTemplateName,String VelocifyLeadId,String createdate, String modifydate) throws IOException{
		emailLogCSV.append("^"+AgentName+"^");
		emailLogCSV.append(',');
        emailLogCSV.append("^"+SendDate+"^");
		emailLogCSV.append(',');
		emailLogCSV.append("^"+EmailTemplateName+"^");
		emailLogCSV.append(',');
		emailLogCSV.append("^"+VelocifyLeadId+"^");
		emailLogCSV.append(',');
		emailLogCSV.append("^"+createdate+"^");
		emailLogCSV.append(',');
		emailLogCSV.append("^"+modifydate+"^");
		emailLogCSV.append("\n");
		System.out.println("Record written for ID for Email Log: " + VelocifyLeadId);
		emailLogCSV.flush();
	}
	
	public void checkForExistence(JSONObject xmlJSONObj, FileWriter writer){
		String firstname="";
    	String lastname="";
    	String home_address="";
    	String city="";
    	String state="";
    	String zip="";
    	String home_phone="";
    	String work_phone="";
    	String email="";
    	String average_monthly_electric_bill="";
    	String electricity_utility_company="";
    	String do_you_own_the_home="";
    	String roof_shade="";
    	String roof_type="";
    	String google_street_view_link="";
    	String sms_phone="";
    	String sms_opt_out="";
    	String netsuite_id="";
    	String lead_source="";
    	String lead_price_paid_$="";
    	String country="";
    	String duplicate_result_from_netsuite="";
    	String home_type="";
    	String preferred_method_of_follow_up="";
    	String lead_generator_comment="";
    	String lead_generator_date="";
    	String lead_generator_time="";
    	String updates_about_sunedison="";
    	String how_much_do_you_want_to_cut_your_bill="";
    	String homeowner_require_financing="";
    	String water_heater_type="";
    	String interested_in="";
    	String originator="";
    	String originator_email="";
    	String sunedison_employee_referrer_firstname="";
    	String sunedison_employee_referrer_lastname="";
    	String sunedison_employee_referrer_email="";
    	String does_your_house_have_an_attic="";
    	String do_you_have_a_central_heating_and_cooling_system="";
    	String primary_phone="";
    	String credit_score="";
    	String custom_1="";
    	String adressee="";
    	String total_system_size_echowatts="";
    	String total_retail_price_quoted="";
    	String contract_cancelled_date="";
    	String interested_in_purchase_type="";
    	String financing_program="";
    	String solar_designer="";
    	String lost_reason="";
    	String lost="";
    	String lost_cancel_comments="";
    	String homeowner_lease_contract_status="";
    	String homeowner_credit_check_status="";
    	String address_2="";
    	String proposal_status="";
    	String campaign_subcategory="";
    	String name="";
    	String address="";
    	String google_street_view_link1="";
    	String email1="";
    	String primary_phone1="";
    	String home_phone1="";
    	String mobile_phone1="";
    	String work_phone1="";
    	String international_phone="";
    	String netsuite_token_id="";
    	String advisor_notes="";
    	String solar_advisor="";
    	String kw_sold="";
    	String system_price="";
    	String energy_consultant="";
    	String energy_consultant_notes="";
    	String other_notes="";
    	String lead_source_manual="";
    	String designer_notes="";
    	String pricing_exceptions="";
    	String velocify_lead_id="";
    	String collected_utility_bill="";
    	String contract_status="";
    	String createdate;
    	String modifydate;
    	String last_distribution_date="";
    	String lead_title="";
    	String status="";
    	String agent_name="";
    	String agent_email="";
    	String campaign="";
    	for(int i=0;i<xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").getJSONObject("Fields").getJSONArray("Field").length();i++){
			JSONObject Field = xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").getJSONObject("Fields").getJSONArray("Field").getJSONObject(i);
	   	 	Object value = Field.get("Value");
	   	 	String fieldType = Field.getString("FieldTitle");
		   	if(fieldType.equals("First Name")){
	 			firstname = "|" + value.toString() + "|";
	 		}
	 		else if(fieldType.equals("Last Name")){
    			lastname = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("Home Address")){
    			home_address = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("City")){
    			city = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("Zip")){
    			zip = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("Home Phone")){
    			home_phone = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("Work Phone")){
    			work_phone = "|" + value.toString( ) + "|";
    		}
    		else if(fieldType.equals("Email")){
    			email = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("Average Monthly Electric Bill")){
    			average_monthly_electric_bill = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("Electricity Utility Company")){
    			electricity_utility_company = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("Do you own the home?")){
    			do_you_own_the_home = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("Roof Shade")){
    			roof_shade = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("Roof Type")){
    			roof_type = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("Google Street View Link")){
    			google_street_view_link = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("SMS Phone")){
    			sms_phone = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("SMS Opt-Out")){
    			sms_opt_out = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("Netsuite ID")){
    			netsuite_id = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("Lead Source")){
    			lead_source = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("Lead Price Paid ($)")){
    			lead_price_paid_$ = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("Country")){
    			country = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("Duplicate Result from Netsuite")){
    			duplicate_result_from_netsuite = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("Home Type")){
    			home_type = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("Preferred Method of Follow-Up")){
    			preferred_method_of_follow_up = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("Lead Generator Comment")){
    			lead_generator_comment = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("Lead Generator Date")){
    			lead_generator_date = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("Lead Generator Time")){
    			lead_generator_time = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("Yes, I would like to receive updates about SunEdis")){
    			updates_about_sunedison = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("How much do you want to cut your bill?")){
    			how_much_do_you_want_to_cut_your_bill = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("Homeowner require financing?")){
    			homeowner_require_financing = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("Water Heater Type")){
    			water_heater_type = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("Interested In")){
    			interested_in = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("Originator")){
    			originator = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("Originator Email")){
    			originator_email = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("SunEdison Employee Referrer First Name")){
    			sunedison_employee_referrer_firstname = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("SunEdison Employee Referrer Last Name")){
    			sunedison_employee_referrer_lastname = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("SunEdison Employee Referrer Email")){
    			sunedison_employee_referrer_email = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("Does your house have an attic?")){
    			does_your_house_have_an_attic = "|" + value.toString() + "|";
    		}
    		else if(fieldType.equals("Do you have a central heating and cooling system?")){
    			do_you_have_a_central_heating_and_cooling_system = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Primary Phone")){
    			primary_phone = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Credit Score")){
    			credit_score = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Custom 1")){
    			custom_1 = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Adressee")){
    			adressee = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Total System Size (Echo Watts)")){
    			total_system_size_echowatts = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Total Retail Price Quoted")){
    			total_retail_price_quoted = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Contract Cancelled Date")){
    			contract_cancelled_date = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Interested in Purchase Type")){
    			interested_in_purchase_type = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Financing Program")){
    			financing_program = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Solar Designer")){
    			solar_designer = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Lost Reason")){
    			lost_reason = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Lost")){
    			lost = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Lost/Cancel Comments")){
    			lost_cancel_comments = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Homeowner Lease Contract Status")){
    			homeowner_lease_contract_status = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Homeowner Credit Check Status")){
    			homeowner_credit_check_status = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Proposal Status")){
    			proposal_status = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Campaign SubCategory")){
    			campaign_subcategory = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Name")){
    			name = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Address")){
    			address = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Google Street View Link1")){
    			google_street_view_link1 = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Email1")){
    			email1 = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Primary Phone1")){
    			primary_phone1 = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Home Phone1")){
    			home_phone1 = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Mobile Phone1")){
    			mobile_phone1 = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Work Phone1")){
    			work_phone1 = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("International Phone")){
    			international_phone = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Netsuite Token ID")){
    			netsuite_token_id = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Advisor Notes")){
    			advisor_notes = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Solar Advisor")){
    			solar_advisor = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("kW Sold")){
    			kw_sold = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("System Price")){
    			system_price = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Energy Consultant")){
    			energy_consultant = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Energy Consultant Notes")){
    			energy_consultant_notes = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Other Notes")){
    			other_notes = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Lead Source Manual")){
    			lead_source_manual = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Designer Notes")){
    			designer_notes = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Pricing Exceptions")){
    			pricing_exceptions = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Velocify Lead ID")){
    			velocify_lead_id = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Collected Utility Bill")){
    			collected_utility_bill = "|" + value.toString() + "|";
    			
    		}
    		else if(fieldType.equals("Contract Status")){
    			contract_status = "|" + value.toString() + "|";
    			
    		}	
		}
    	
		createdate = "|" + xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("CreateDate").toString() + "|";
		modifydate = "|" + xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("ModifyDate").toString() + "|";
		last_distribution_date = "|" + xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("LastDistributionDate").toString() + "|";
		lead_title = "|" + xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").get("LeadTitle").toString() + "|";
		status = "|" + xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").getJSONObject("Status").get("StatusTitle").toString() + "|";
		if(xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").has("Agent")){
			agent_name = "|" + xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").getJSONObject("Agent").get("AgentName").toString() + "|";
			agent_email = "|" + xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").getJSONObject("Agent").get("AgentEmail").toString() + "|";
		}
		campaign = "|" + xmlJSONObj.getJSONObject("Leads").getJSONObject("Lead").getJSONObject("Campaign").get("CampaignTitle").toString() + "|";
		//System.out.println(firstname + " : " + lastname);
		//insertIntoRedshift(firstname,lastname,home_address,city,state,zip,home_phone,work_phone,email,average_monthly_electric_bill,electricity_utility_company,do_you_own_the_home,roof_shade,roof_type,google_street_view_link,sms_phone,sms_opt_out,netsuite_id,lead_source,lead_price_paid_$,country,duplicate_result_from_netsuite,home_type,preferred_method_of_follow_up,lead_generator_comment,lead_generator_date,lead_generator_time,updates_about_sunedison,how_much_do_you_want_to_cut_your_bill,homeowner_require_financing,water_heater_type,interested_in,originator,originator_email,sunedison_employee_referrer_firstname,sunedison_employee_referrer_lastname,sunedison_employee_referrer_email,does_your_house_have_an_attic,do_you_have_a_central_heating_and_cooling_system,primary_phone,credit_score,custom_1,adressee,total_system_size_echowatts,total_retail_price_quoted,contract_cancelled_date,interested_in_purchase_type,financing_program,solar_designer,lost_reason,lost,lost_cancel_comments,homeowner_lease_contract_status,homeowner_credit_check_status,address_2,proposal_status,campaign_subcategory,name,address,google_street_view_link1,email1,primary_phone1,home_phone1,mobile_phone1,work_phone1,international_phone,netsuite_token_id,advisor_notes,solar_advisor,kw_sold,system_price,energy_consultant,energy_consultant_notes,other_notes,lead_source_manual,designer_notes,pricing_exceptions,velocify_lead_id,collected_utility_bill,contract_status);
		try{
			
			generateCSVForMainLeads(writer,firstname,lastname,home_address,city,state,zip,home_phone,work_phone,email,average_monthly_electric_bill,electricity_utility_company,do_you_own_the_home,roof_shade,roof_type,google_street_view_link,sms_phone,sms_opt_out,netsuite_id,lead_source,lead_price_paid_$,country,duplicate_result_from_netsuite,home_type,preferred_method_of_follow_up,lead_generator_comment,lead_generator_date,lead_generator_time,updates_about_sunedison,how_much_do_you_want_to_cut_your_bill,homeowner_require_financing,water_heater_type,interested_in,originator,originator_email,sunedison_employee_referrer_firstname,sunedison_employee_referrer_lastname,sunedison_employee_referrer_email,does_your_house_have_an_attic,do_you_have_a_central_heating_and_cooling_system,primary_phone,credit_score,custom_1,adressee,total_system_size_echowatts,total_retail_price_quoted,contract_cancelled_date,interested_in_purchase_type,financing_program,solar_designer,lost_reason,lost,lost_cancel_comments,homeowner_lease_contract_status,homeowner_credit_check_status,address_2,proposal_status,campaign_subcategory,name,address,google_street_view_link1,email1,primary_phone1,home_phone1,mobile_phone1,work_phone1,international_phone,netsuite_token_id,advisor_notes,solar_advisor,kw_sold,system_price,energy_consultant,energy_consultant_notes,other_notes,lead_source_manual,designer_notes,pricing_exceptions,velocify_lead_id,collected_utility_bill,contract_status,createdate,modifydate,last_distribution_date,lead_title,status,agent_name,agent_email,campaign);
			
			//writer.append()
			//System.out.println(xmlJSONObj);
		}
		catch(Exception e){
			System.out.println(e);
		}
		
	}
	
	public void generateCSVForMainLeads(FileWriter writer, String firstname,String lastname,String home_address,String city,String state,String zip,String home_phone,String work_phone,String email,String average_monthly_electric_bill,String electricity_utility_company,String do_you_own_the_home,String roof_shade,String roof_type,String google_street_view_link,String sms_phone,String sms_opt_out,String netsuite_id,String lead_source,String lead_price_paid_$,String country,String duplicate_result_from_netsuite,String home_type,String preferred_method_of_follow_up,String lead_generator_comment,String lead_generator_date,String lead_generator_time,String updates_about_sunedison,String how_much_do_you_want_to_cut_your_bill,String homeowner_require_financing,String water_heater_type,String interested_in,String originator,String originator_email,String sunedison_employee_referrer_firstname,String sunedison_employee_referrer_lastname,String sunedison_employee_referrer_email,String does_your_house_have_an_attic,String do_you_have_a_central_heating_and_cooling_system,String primary_phone,String credit_score,String custom_1,String adressee,String total_system_size_echowatts,String total_retail_price_quoted,String contract_cancelled_date,String interested_in_purchase_type,String financing_program,String solar_designer,String lost_reason,String lost,String lost_cancel_comments,String homeowner_lease_contract_status,String homeowner_credit_check_status,String address_2,String proposal_status,String campaign_subcategory,String name,String address,String google_street_view_link1,String email1,String primary_phone1,String home_phone1,String mobile_phone1,String work_phone1,String international_phone,String netsuite_token_id,String advisor_notes,String solar_advisor,String kw_sold,String system_price,String energy_consultant,String energy_consultant_notes,String other_notes,String lead_source_manual,String designer_notes,String pricing_exceptions,String velocify_lead_id,String collected_utility_bill,String contract_status, String createdate, String modifydate, String last_distribution_date, String lead_title, String status, String agent_name, String agent_email, String campaign) throws IOException{
		try{
			//FileWriter writer = new FileWriter("c:\\leads.csv");
			writer.append(firstname);
			writer.append(',');
			writer.append(lastname);
			writer.append(',');
			writer.append(home_address);
			writer.append(',');
			writer.append(city);
			writer.append(',');
			writer.append(state);
			writer.append(',');
			writer.append(zip);
			writer.append(',');
			writer.append(home_phone);
			writer.append(',');
			writer.append(work_phone);
			writer.append(',');
			writer.append(email);
			writer.append(',');
			writer.append(average_monthly_electric_bill);
			writer.append(',');
			writer.append(electricity_utility_company);
			writer.append(',');
			writer.append(do_you_own_the_home);
			writer.append(',');
			writer.append(roof_shade);
			writer.append(',');
			writer.append(roof_type);
			writer.append(',');
			writer.append(google_street_view_link);
			writer.append(',');
			writer.append(sms_phone);
			writer.append(',');
			writer.append(sms_opt_out);
			writer.append(',');
			writer.append(netsuite_id);
			writer.append(',');
			writer.append(lead_source);
			writer.append(',');
			writer.append(lead_price_paid_$);
			writer.append(',');
			writer.append(country);
			writer.append(',');
			writer.append(duplicate_result_from_netsuite);
			writer.append(',');
			writer.append(home_type);
			writer.append(',');
			writer.append(preferred_method_of_follow_up);
			writer.append(',');
			writer.append(lead_generator_comment);
			writer.append(',');
			writer.append(lead_generator_date);
			writer.append(',');
			writer.append(lead_generator_time);
			writer.append(',');
			writer.append(updates_about_sunedison);
			writer.append(',');
			writer.append(how_much_do_you_want_to_cut_your_bill);
			writer.append(',');
			writer.append(homeowner_require_financing);
			writer.append(',');
			writer.append(water_heater_type);
			writer.append(',');
			writer.append(interested_in);
			writer.append(',');
			writer.append(originator);
			writer.append(',');
			writer.append(originator_email);
			writer.append(',');
			writer.append(sunedison_employee_referrer_firstname);
			writer.append(',');
			writer.append(sunedison_employee_referrer_lastname);
			writer.append(',');
			writer.append(sunedison_employee_referrer_email);
			writer.append(',');
			writer.append(does_your_house_have_an_attic);
			writer.append(',');
			writer.append(do_you_have_a_central_heating_and_cooling_system);
			writer.append(',');
			writer.append(primary_phone);
			writer.append(',');
			writer.append(credit_score);
			writer.append(',');
			writer.append(custom_1);
			writer.append(',');
			writer.append(adressee);
			writer.append(',');
			writer.append(total_system_size_echowatts);
			writer.append(',');
			writer.append(total_retail_price_quoted);
			writer.append(',');
			writer.append(contract_cancelled_date);
			writer.append(',');
			writer.append(interested_in_purchase_type);
			writer.append(',');
			writer.append(financing_program);
			writer.append(',');
			writer.append(solar_designer);
			writer.append(',');
			writer.append(lost_reason);
			writer.append(',');
			writer.append(lost);
			writer.append(',');
			writer.append(lost_cancel_comments);
			writer.append(',');
			writer.append(homeowner_lease_contract_status);
			writer.append(',');
			writer.append(homeowner_credit_check_status);
			writer.append(',');
			writer.append(address_2);
			writer.append(',');
			writer.append(proposal_status);
			writer.append(',');
			writer.append(campaign_subcategory);
			writer.append(',');
			writer.append(name);
			writer.append(',');
			writer.append(address);
			writer.append(',');
			writer.append(google_street_view_link1);
			writer.append(',');
			writer.append(email1);
			writer.append(',');
			writer.append(primary_phone1);
			writer.append(',');
			writer.append(home_phone1);
			writer.append(',');
			writer.append(mobile_phone1);
			writer.append(',');
			writer.append(work_phone1);
			writer.append(',');
			writer.append(international_phone);
			writer.append(',');
			writer.append(netsuite_token_id);
			writer.append(',');
			writer.append(advisor_notes);
			writer.append(',');
			writer.append(solar_advisor);
			writer.append(',');
			writer.append(kw_sold);
			writer.append(',');
			writer.append(system_price);
			writer.append(',');
			writer.append(energy_consultant);
			writer.append(',');
			writer.append(energy_consultant_notes);
			writer.append(',');
			writer.append(other_notes);
			writer.append(',');
			writer.append(lead_source_manual);
			writer.append(',');
			writer.append(designer_notes);
			writer.append(',');
			writer.append(pricing_exceptions);
			writer.append(',');
			writer.append(velocify_lead_id);
			writer.append(',');
			writer.append(collected_utility_bill);
			writer.append(',');
			writer.append(contract_status);
			writer.append(',');
			writer.append(createdate);
			writer.append(',');
			writer.append(modifydate);
			writer.append(',');
			writer.append(last_distribution_date);
			writer.append(',');
			writer.append(lead_title);
			writer.append(',');
			writer.append(status);
			writer.append(',');
			writer.append(agent_name);
			writer.append(',');
			writer.append(agent_email);
			writer.append(',');
			writer.append(campaign);
			writer.append('\n');
			System.out.println("Record written for ID: " + velocify_lead_id);
			writer.flush();
		}	
		catch(IOException e){
			System.out.println(e);
		}
	}
	
	public void insertIntoRedshift(String firstname,String lastname,String home_address,String city,String state,String zip,String home_phone,String work_phone,String email,String average_monthly_electric_bill,String electricity_utility_company,String do_you_own_the_home,String roof_shade,String roof_type,String google_street_view_link,String sms_phone,String sms_opt_out,String netsuite_id,String lead_source,String lead_price_paid_$,String country,String duplicate_result_from_netsuite,String home_type,String preferred_method_of_follow_up,String lead_generator_comment,String lead_generator_date,String lead_generator_time,String updates_about_sunedison,String how_much_do_you_want_to_cut_your_bill,String homeowner_require_financing,String water_heater_type,String interested_in,String originator,String originator_email,String sunedison_employee_referrer_firstname,String sunedison_employee_referrer_lastname,String sunedison_employee_referrer_email,String does_your_house_have_an_attic,String do_you_have_a_central_heating_and_cooling_system,String primary_phone,String credit_score,String custom_1,String adressee,String total_system_size_echowatts,String total_retail_price_quoted,String contract_cancelled_date,String interested_in_purchase_type,String financing_program,String solar_designer,String lost_reason,String lost,String lost_cancel_comments,String homeowner_lease_contract_status,String homeowner_credit_check_status,String address_2,String proposal_status,String campaign_subcategory,String name,String address,String google_street_view_link1,String email1,String primary_phone1,String home_phone1,String mobile_phone1,String work_phone1,String international_phone,String netsuite_token_id,String advisor_notes,String solar_advisor,String kw_sold,String system_price,String energy_consultant,String energy_consultant_notes,String other_notes,String lead_source_manual,String designer_notes,String pricing_exceptions,String velocify_lead_id,String collected_utility_bill,String contract_status){
		String dbURL = "jdbc:postgresql://sunedisondatawarehouse.cgnr3c8sn1sz.us-west-2.redshift.amazonaws.com:5439/sunedison";
		String MasterUsername = "abhardwaj";
	    String MasterUserPassword = "Master12"; 
	    Connection conn = null;
        Statement stmt = null;
        try{
            Class.forName("com.amazon.redshift.jdbc4.Driver");
            //System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(dbURL,MasterUsername,MasterUserPassword);
            stmt = conn.createStatement();
            String sql = "insert into leads_temp (firstname,lastname,home_address,city,state,zip,home_phone,work_phone,email,average_monthly_electric_bill,electricity_utility_company,do_you_own_the_home,roof_shade,roof_type,google_street_view_link,sms_phone,sms_opt_out,netsuite_id,lead_source,lead_price_paid_$,country,duplicate_result_from_netsuite,home_type,preferred_method_of_follow_up,lead_generator_comment,lead_generator_date,lead_generator_time,updates_about_sunedison,how_much_do_you_want_to_cut_your_bill,homeowner_require_financing,water_heater_type,interested_in,originator,originator_email,sunedison_employee_referrer_firstname,sunedison_employee_referrer_lastname,sunedison_employee_referrer_email,does_your_house_have_an_attic,do_you_have_a_central_heating_and_cooling_system,primary_phone,credit_score,custom_1,adressee,total_system_size_echowatts,total_retail_price_quoted,contract_cancelled_date,interested_in_purchase_type,financing_program,solar_designer,lost_reason,lost,lost_cancel_comments,homeowner_lease_contract_status,homeowner_credit_check_status,address_2,proposal_status,campaign_subcategory,name,address,google_street_view_link1,email1,primary_phone1,home_phone1,mobile_phone1,work_phone1,international_phone,netsuite_token_id,advisor_notes,solar_advisor,kw_sold,system_price,energy_consultant,energy_consultant_notes,other_notes,lead_source_manual,designer_notes,pricing_exceptions,velocify_lead_id,collected_utility_bill,contract_status) values ('"+firstname+"','" + lastname + "','"+ home_address + "','"+ city + "','"+ state + "','"+ zip + "','"+ home_phone + "','"+ work_phone + "','"+ email + "','"+ average_monthly_electric_bill + "','"+ electricity_utility_company + "','"+ do_you_own_the_home + "','"+ roof_shade + "','"+ roof_type + "','"+ google_street_view_link + "','"+ sms_phone + "','"+ sms_opt_out + "','"+ netsuite_id + "','"+ lead_source + "','"+ lead_price_paid_$ + "','"+ country + "','"+ duplicate_result_from_netsuite + "','"+ home_type + "','"+ preferred_method_of_follow_up + "','"+ lead_generator_comment + "','"+ lead_generator_date + "','"+ lead_generator_time + "','"+ updates_about_sunedison + "','"+ how_much_do_you_want_to_cut_your_bill + "','"+ homeowner_require_financing + "','"+ water_heater_type + "','"+ interested_in + "','"+ originator + "','"+ originator_email + "','"+ sunedison_employee_referrer_firstname + "','"+ sunedison_employee_referrer_lastname + "','"+ sunedison_employee_referrer_email + "','"+ does_your_house_have_an_attic + "','"+ do_you_have_a_central_heating_and_cooling_system + "','"+ primary_phone + "','"+ credit_score + "','"+ custom_1 + "','"+ adressee + "','"+ total_system_size_echowatts + "','"+ total_retail_price_quoted + "','"+ contract_cancelled_date + "','"+ interested_in_purchase_type + "','"+ financing_program + "','"+ solar_designer + "','"+ lost_reason + "','"+ lost + "','"+ lost_cancel_comments + "','"+ homeowner_lease_contract_status + "','"+ homeowner_credit_check_status + "','"+ address_2 + "','"+ proposal_status + "','"+ campaign_subcategory + "','"+ name + "','"+ address + "','"+ google_street_view_link1 + "','"+ email1 + "','"+ primary_phone1 + "','"+ home_phone1 + "','"+ mobile_phone1 + "','"+ work_phone1 + "','"+ international_phone + "','"+ netsuite_token_id + "','"+ advisor_notes + "','"+ solar_advisor + "','"+ kw_sold + "','"+ system_price + "','"+ energy_consultant + "','"+ energy_consultant_notes + "','"+ other_notes + "','"+ lead_source_manual + "','"+ designer_notes + "','"+ pricing_exceptions + "','"+ velocify_lead_id + "','"+ collected_utility_bill + "','"+ contract_status + "');";
            stmt.executeUpdate(sql);
            System.out.println("Row Inserted for ID: "+ velocify_lead_id);
            stmt.close();
            conn.close();
         }catch(Exception ex){
            //For convenience, handle all errors here.
            ex.printStackTrace();
         }finally{
            //Finally block to close resources.
            try{
               if(stmt!=null)
                  stmt.close();
            }catch(Exception ex){
            }// nothing we can do
            try{
               if(conn!=null)
                  conn.close();
            }catch(Exception ex){
               ex.printStackTrace();
            }
         }
	}
}

//String abc = "SELECT CASE WHEN CUSTOMER.FIRSTNAME NOT like 'TL%' and customer.firstname not like 'Tl%' and customer.firstname not like 'tL%' and customer.firstname not like 'tl%' THEN CUSTOMER.FIRSTNAME END AS FIRSTNAME,case when customer.lastname not like 'TL%' and customer.lastname not like 'Tl%' and customer.lastname not like 'tL%' and customer.lastname not like 'tl%' then customer.lastname end as lastname, CASE WHEN sales_order.number is not null then sales_order.number end as SO_Number,CASE WHEN sales_order.current_promise_date is not null then sales_order.current_promise_date end as SO_Promised_date,CASE WHEN sales_order.orderstatus is not null then sales_order.orderstatus end as SO_Status,CASE WHEN sales_order.actual_delivery_date is not null then sales_order.actual_delivery_date end as Actual_Del_Date,CASE WHEN NVL2(sales_order.datecreated, TO_CHAR(sales_order.datecreated, 'YYYY') || '_Q' || TO_CHAR(sales_order.datecreated, 'Q'), Null) IS NOT NULL THEN NVL2(sales_order.datecreated, TO_CHAR(sales_order.datecreated, 'YYYY') || '_Q' || TO_CHAR(sales_order.datecreated, 'Q'), Null) END AS SO_Created_Q,CASE WHEN NVL2(sales_order.datecreated, TO_CHAR(sales_order.datecreated, 'IYYY') || '_' || TO_CHAR(sales_order.datecreated, 'IW'), Null) IS NOT NULL THEN NVL2(sales_order.datecreated, TO_CHAR(sales_order.datecreated, 'IYYY') || '_' || TO_CHAR(sales_order.datecreated, 'IW'), Null) END AS SO_Created_WW, CASE WHEN TO_DATE(sales_order.datecreated,'YYYY-MM-DD') IS NOT NULL THEN TO_DATE(sales_order.datecreated,'YYYY-MM-DD') END AS SO_Created_Date,CASE WHEN NVL2(customer.date_created, TO_CHAR(customer.date_created, 'YYYY') || '_Q' || TO_CHAR(customer.date_created, 'Q'), Null) IS NOT NULL THEN NVL2(customer.date_created, TO_CHAR(customer.date_created, 'YYYY') || '_Q' || TO_CHAR(customer.date_created, 'Q'), Null) END AS Lead_Create_Q,case when NVL2(customer.date_created, TO_CHAR(customer.date_created, 'IYYY') || '_' || TO_CHAR(customer.date_created, 'IW'), Null) is not null then NVL2(customer.date_created, TO_CHAR(customer.date_created, 'IYYY') || '_' || TO_CHAR(customer.date_created, 'IW'), Null) end as Lead_Create_WW, CASE WHEN TO_Date(customer.date_created,'YYYY-MM-DD') is not null then TO_Date(customer.date_created,'YYYY-MM-DD') end as Lead_Create_Date,CASE WHEN sales_order.actual_delivery_date is not null then sales_order.actual_delivery_date end as SO_Delivery_Date, CASE WHEN sales_order.pv_total_kw_ordered is not null then sales_order.pv_total_kw_ordered end as KW_ordered, CASE WHEN (sales_order.pv_total_kw_ordered IS NOT NULL) THEN sales_order.pv_total_kw_ordered ELSE customer.proposed_system_size_pvkwh END AS KW_System, CASE WHEN (NVL(site.milestone_2_payment_approval_, site.installer2_payment_approval_d) IS NOT NULL AND sales_order.actual_delivery_date IS NOT NULL) THEN TO_DATE(NVL(site.milestone_2_payment_approval_, site.installer2_payment_approval_d),'YYYY-MM-DD') - sales_order.actual_delivery_date ELSE NULL END as Delivery_2_MP2, CASE WHEN (sales_order.datecreated IS NOT NULL AND sales_order.actual_delivery_date IS NOT NULL) THEN sales_order.actual_delivery_date - TO_DATE(sales_order.datecreated,'YYYY-MM-DD') ELSE NULL END AS SO_Created_2_Delivery, CASE WHEN (customer.homeowner_proj_document_sta_id IN ('Signed by Partner', 'Signed by SunEdison') AND sales_order.datecreated IS NOT NULL) THEN TO_DATE(sales_order.datecreated,'YYYY-MM-DD') - customer.proj_definition_doc_last_upda ELSE NULL END as PDA_2_SO_Create, CASE WHEN SITE.final_completion_certificate IS NOT NULL THEN SITE.final_completion_certificate END AS final_completion_certificate,CASE WHEN SITE.inverter_quantity IS NOT NULL THEN SITE.inverter_quantity END AS inverter_quantity,CASE WHEN SITE.inverter_model_1_id IS NOT NULL THEN SITE.inverter_model_1_id END AS inverter_model_1,CASE WHEN SITE.inverter_mfr_1_id IS NOT NULL THEN SITE.inverter_mfr_1_id END AS inverter_mfr_1,CASE WHEN SITE.pv_modules_quantity IS NOT NULL THEN SITE.pv_modules_quantity END AS pv_modules_quantity,CASE WHEN SITE.pv_module_model IS NOT NULL THEN SITE.pv_module_model END AS pv_module_model,CASE WHEN CUSTOMER.PDA_SIGNED_DATE IS NOT NULL THEN CUSTOMER.PDA_SIGNED_DATE END AS PDA_SIGNED_DATE,CASE WHEN CUSTOMER.subsidiary_no_hierarchy IS NOT NULL THEN CUSTOMER.subsidiary_no_hierarchy END AS SUBSIDIARY_NO_HIERARCHY,CASE WHEN SITE.STREET_1 IS NOT NULL THEN SITE.STREET_1 END AS STREET_1,CASE WHEN SITE.CITY IS NOT NULL THEN SITE.CITY END AS CITY,CASE WHEN SITE.ZIP IS NOT NULL THEN SITE.ZIP END AS ZIP,CASE WHEN SITE.TRANCHE_ID IS NOT NULL THEN SITE.TRANCHE_ID END AS TRANCHE_ID,CASE WHEN SITE.SITE_VISIT_DATE IS NOT NULL THEN SITE.SITE_VISIT_DATE END AS SITE_VISIT_DATE,CASE WHEN CUSTOMER.contract_canceled_date IS NOT NULL THEN CUSTOMER.contract_canceled_date END AS CONTRACT_CANCELED_DATE,CASE WHEN CUSTOMER.FULLY_EXECUTED_DATE IS NOT NULL THEN CUSTOMER.FULLY_EXECUTED_DATE END AS FULLY_EXECUTED_DATE,CASE WHEN CUSTOMER.executed_assign_agrmnt_updat IS NOT NULL THEN CUSTOMER.executed_assign_agrmnt_updat END AS ASSIGNMENT_AGMNT_UPDATE_DATE,CASE WHEN CUSTOMER.executed_assgn_agmnt_status IS NOT NULL THEN CUSTOMER.executed_assgn_agmnt_status END AS ASSIGNMENT_AGMNT_STATUS,CASE WHEN SITE.commissioned_date IS NOT NULL THEN SITE.commissioned_date END AS COMMISSIONED_DATE,CASE WHEN CUSTOMER.proj_definition_doc_last_upda IS NOT NULL THEN CUSTOMER.proj_definition_doc_last_upda END AS PDA_LAST_UPDATE,CASE WHEN CUSTOMER.homeowner_proj_document_sta_id IS NOT NULL THEN CUSTOMER.homeowner_proj_document_sta_id END AS PDA_STATUS,CASE WHEN SITE.site_visit_status_id IS NOT NULL THEN SITE.site_visit_status_id END AS SITE_VISIT_STATUS,CASE WHEN CUSTOMER.lease_contract_status_last_up IS NOT NULL THEN CUSTOMER.lease_contract_status_last_up END AS CONTRACT_STATUS_UPDATE,CASE WHEN CUSTOMER.homeowner_lease_contract_st_id IS NOT NULL THEN CUSTOMER.homeowner_lease_contract_st_id END AS CONTRACT_STATUS,CASE WHEN CUSTOMER.credit_check_last_update_date IS NOT NULL THEN CUSTOMER.credit_check_last_update_date END AS CC_STATUS_UPDATE,CASE WHEN CUSTOMER.homeowner_credit_check_status IS NOT NULL THEN CUSTOMER.homeowner_credit_check_status END AS CREDIT_CHECK_STATUS,CASE WHEN CUSTOMER.proposal_status_last_update_date IS NOT NULL THEN CUSTOMER.proposal_status_last_update_date END AS PROPOSAL_STATUS_UPDATE,CASE WHEN CUSTOMER.proposed_system_size_pvkwh IS NOT NULL THEN CUSTOMER.proposed_system_size_pvkwh END AS KW_PROPOSED,CASE WHEN (CUSTOMER.RECORD IS NOT NULL) THEN CUSTOMER.RECORD END AS customer_id, CASE WHEN (SITE.TSM IS NOT NULL) THEN SITE.TSM END AS TSM, CASE WHEN SITE.RECORD IS NOT NULL THEN SITE.RECORD END AS SITE_ID, CASE WHEN CUSTOMER.PROPOSAL_STATUS_ID IS NOT NULL THEN CUSTOMER.PROPOSAL_STATUS_ID END AS PROPOSAL_STATUS, CASE WHEN SITE.SAI_INSTALLER IS NOT NULL THEN SITE.SAI_INSTALLER END AS SAI_INSTALLER, CASE WHEN ((CUSTOMER.FULLY_EXECUTED_DATE IS NOT NULL) AND (CUSTOMER.CONTRACT_CANCELED_DATE IS NOT NULL)) THEN 'Contract Cancelled' WHEN SITE.MILESTONE_3_PAYMENT_APPROVAL_ IS NOT NULL THEN 'Pending Dropdown' WHEN SITE.MILESTONE_2_PAYMENT_APPROVAL_ IS NOT NULL THEN 'Pending PTO' WHEN sales_order.actual_delivery_date IS NOT NULL AND customer.financing_program IS NOT NULL THEN 'Pending Install Complete' WHEN sales_order.number IS NOT NULL AND customer.financing_program IS NOT NULL THEN 'Pending Delivery' WHEN CUSTOMER.HOMEOWNER_PROJ_DOCUMENT_STA_ID IN ('Signed by Partner', 'Signed by SunEdison') THEN 'Pending So Create' WHEN ((CUSTOMER.FULLY_EXECUTED_DATE IS NOT NULL)  AND (CUSTOMER.CONTRACT_CANCELED_DATE IS NULL)) THEN 'Pending PDA' WHEN CUSTOMER.HOMEOWNER_LEASE_CONTRACT_ST_ID LIKE 'Signed by HO%' THEN 'Pending Contract Completion' WHEN CUSTOMER.PROPOSAL_STATUS_ID IN ('Proposal Accepted', 'Proposal Initiated', 'Proposal Signed') THEN 'Pending Contract' WHEN customer.proposal_status_id IS NULL AND (customer.financing_program IS NOT NULL OR customer.purchase_type_id IN ('Lease - Monthly', 'Loan', 'PPA')) THEN 'Pending Proposal' ELSE NULL END AS STAGE, CASE WHEN CUSTOMER.PARTNER_SUB_TYPE_ID = 'SALES ENGINE (Seller)' THEN (CASE WHEN CUSTOMER.HOMEOWNER_PROJ_DOCUMENT_STA_ID= 'Signed by SunEdison' THEN (CASE WHEN SITE.SITE_VISIT_STATUS_ID = 'Completed' THEN (CASE WHEN CUSTOMER.HOMEOWNER_PROJ_DOCUMENT_STA_ID IS NOT NULL THEN ('PDA: ' || CUSTOMER.HOMEOWNER_PROJ_DOCUMENT_STA_ID) ELSE 'Site Survey : Completed' END) ELSE (CASE WHEN SITE.SITE_VISIT_STATUS_ID IS NULL THEN 'Site Survey: Not Scheduled' ELSE ('Site Survey: ' || SITE.SITE_VISIT_STATUS_ID) END) END) END) END AS STAGE_SAI_PLACEHOLDER, CASE WHEN (CUSTOMER.FULLY_EXECUTED_DATE IS NOT NULL) AND (CUSTOMER.CONTRACT_CANCELED_DATE IS NOT NULL) THEN 'Contract Cancelled' WHEN SITE.TRANCHE_ID IS NOT NULL THEN 'Tranche Complete' WHEN SITE.MILESTONE_3_PAYMENT_APPROVAL_ IS NOT NULL THEN 'Pending Dropdown' WHEN SITE.MILESTONE_2_PAYMENT_APPROVAL_ IS NOT NULL THEN 'Pending PTO' WHEN sales_order.actual_delivery_date IS NOT NULL AND customer.financing_program IS NOT NULL THEN 'Pending Install Complete' WHEN sales_order.number IS NOT NULL AND customer.financing_program IS NOT NULL THEN 'Pending Delivery' WHEN CUSTOMER.HOMEOWNER_PROJ_DOCUMENT_STA_ID IN ('Signed by Partner', 'Signed by SunEdison') THEN 'Pending So Create' WHEN CUSTOMER.FULLY_EXECUTED_DATE IS NOT NULL AND CUSTOMER.CONTRACT_CANCELED_DATE IS NULL THEN 'Pending PDA' WHEN CUSTOMER.HOMEOWNER_LEASE_CONTRACT_ST_ID LIKE 'Signed by HO%' THEN 'Pending Contract Completion'  WHEN CUSTOMER.PROPOSAL_STATUS_ID IN ('Proposal Accepted', 'Proposal Initiated', 'Proposal Signed') THEN 'Pending Contract' WHEN customer.proposal_status_id IS NULL AND (customer.financing_program IS NOT NULL OR customer.purchase_type_id IN ('Lease - Monthly', 'Loan', 'PPA')) THEN 'Pending Proposal' ELSE NULL END AS STAGE_SOP_PLACEHOLDER, NVL2(CUSTOMER.FULLY_EXECUTED_DATE, TO_CHAR(CUSTOMER.FULLY_EXECUTED_DATE, 'IYYY') || '_' || TO_CHAR(CUSTOMER.FULLY_EXECUTED_DATE, 'IW'), NULL) AS FULLY_EXECUTED_WW, CASE WHEN ( SITE.SITE_VISIT_STATUS_ID IN ('Completed')) THEN CUSTOMER.FULLY_EXECUTED_DATE - SITE.SITE_VISIT_DATE ELSE NULL END AS CONTRACT_2_SITE_VISIT, CASE WHEN ( CUSTOMER.HOMEOWNER_PROJ_DOCUMENT_STA_ID IN ('Signed by Partner', 'Signed by SunEdison')) THEN CUSTOMER.PROJ_DEFINITION_DOC_LAST_UPDA - CUSTOMER.FULLY_EXECUTED_DATE ELSE NULL END AS CONTRACT_2_PDA, CASE WHEN (NVL(SITE.MILESTONE_2_PAYMENT_APPROVAL_, INSTALLER2_PAYMENT_APPROVAL_D) IS NOT NULL AND NVL(SITE.MILESTONE_3_PAYMENT_APPROVAL_,SITE.INSTALLER3_PAYMENT_APPROVAL_D) IS NOT NULL) THEN TO_DATE(NVL(SITE.MILESTONE_3_PAYMENT_APPROVAL_,SITE.INSTALLER3_PAYMENT_APPROVAL_D),'YYYY-MM-DD') - TO_DATE(NVL(SITE.MILESTONE_2_PAYMENT_APPROVAL_,SITE.INSTALLER2_PAYMENT_APPROVAL_D),'YYYY-MM-DD') ELSE NULL END AS MP2_2_MP3, CASE WHEN (NVL(SITE.MILESTONE_2_PAYMENT_APPROVAL_,SITE.INSTALLER2_PAYMENT_APPROVAL_D) IS NOT NULL) THEN TO_DATE(NVL(SITE.MILESTONE_2_PAYMENT_APPROVAL_,SITE.INSTALLER2_PAYMENT_APPROVAL_D),'YYYY-MM-DD') - CUSTOMER.FULLY_EXECUTED_DATE ELSE NULL END AS CONTRACT_2_MP2, CASE WHEN (NVL(SITE.MILESTONE_3_PAYMENT_APPROVAL_,SITE.INSTALLER3_PAYMENT_APPROVAL_D) IS NOT NULL) THEN TO_DATE(NVL(SITE.MILESTONE_3_PAYMENT_APPROVAL_,SITE.INSTALLER3_PAYMENT_APPROVAL_D),'YYYY-MM-DD') - CUSTOMER.FULLY_EXECUTED_DATE ELSE NULL END AS CONTRACT_2_MP3, CASE WHEN (SITE.SALES_CHANNEL_ID IS NOT NULL) THEN SITE.SALES_CHANNEL_ID WHEN CUSTOMER.ASSIGNED_PARTNER_ID LIKE 'SunEdison Inside%' THEN 'IS' WHEN CUSTOMER.ASSIGNED_PARTNER_ID IN ('Brite Energy', 'Complete Solar Solution Inc') THEN 'KP' WHEN CUSTOMER.ASSIGNED_PARTNER_ID IN ('Clear Solar - Sales Engine') THEN 'SE' WHEN CUSTOMER.ASSIGNED_PARTNER_ID IN ('Evolve Solar') THEN 'EV' END AS CHANNEL, CASE WHEN NVL(SITE.PARTNER_ID,CUSTOMER.ASSIGNED_PARTNER_ID) NOT IN ('API-TestPartner_Prod','Test Partner, Inc..') THEN NVL(SITE.PARTNER_ID,CUSTOMER.ASSIGNED_PARTNER_ID) END AS PARTNER, NVL(SITE.MILESTONE_2_PAYMENT_APPROVAL_,SITE.INSTALLER2_PAYMENT_APPROVAL_D) AS MP2_APPROVAL_DATE, NVL(SITE.MILESTONE_3_PAYMENT_APPROVAL_, SITE.INSTALLER3_PAYMENT_APPROVAL_D) AS MP3_APPROVAL_DATE, NVL2(CUSTOMER.CREDIT_CHECK_LAST_UPDATE_DATE, TO_CHAR(CUSTOMER.CREDIT_CHECK_LAST_UPDATE_DATE, 'IYYY') || '_' || TO_CHAR(CUSTOMER.CREDIT_CHECK_LAST_UPDATE_DATE, 'IW'), NULL) AS CC_STATUS_WW, NVL2(CUSTOMER.CREDIT_CHECK_LAST_UPDATE_DATE, TO_CHAR(CUSTOMER.CREDIT_CHECK_LAST_UPDATE_DATE, 'YYYY') || '_Q' || TO_CHAR(CUSTOMER.CREDIT_CHECK_LAST_UPDATE_DATE, 'Q'), NULL) AS CC_STATUS_Q, NVL2(CUSTOMER.LEASE_CONTRACT_STATUS_LAST_UP, TO_CHAR(CUSTOMER.LEASE_CONTRACT_STATUS_LAST_UP, 'IYYY') || '_' || TO_CHAR(CUSTOMER.LEASE_CONTRACT_STATUS_LAST_UP, 'IW'), NULL) AS CONTRACT_STATUS_UPDATE_WW, NVL2(CUSTOMER.LEASE_CONTRACT_STATUS_LAST_UP, TO_CHAR(CUSTOMER.LEASE_CONTRACT_STATUS_LAST_UP, 'YYYY') || '_Q' || TO_CHAR(CUSTOMER.LEASE_CONTRACT_STATUS_LAST_UP, 'Q'), NULL) AS CONTRACT_STATUS_UPDATE_Q, NVL2(CUSTOMER.FULLY_EXECUTED_DATE, TO_CHAR(CUSTOMER.FULLY_EXECUTED_DATE, 'YYYY') || '_Q' || TO_CHAR(CUSTOMER.FULLY_EXECUTED_DATE, 'Q'), NULL) AS FULLY_EXECUTED_Q, NVL2(SITE.SITE_VISIT_DATE, TO_CHAR(SITE.SITE_VISIT_DATE, 'IYYY') || '_' || TO_CHAR(SITE.SITE_VISIT_DATE, 'IW'), NULL) AS SITE_VISIT_WW, NVL2(SITE.SITE_VISIT_DATE, TO_CHAR(SITE.SITE_VISIT_DATE, 'YYYY') || '_Q' || TO_CHAR(SITE.SITE_VISIT_DATE, 'Q'), NULL) AS SITE_VISIT_Q, CASE WHEN (CUSTOMER.HOMEOWNER_PROJ_DOCUMENT_STA_ID IN ('Signed by Partner', 'Signed by SunEdison')) THEN CUSTOMER.PROJ_DEFINITION_DOC_LAST_UPDA ELSE NULL END AS PDA_COMPLETE_DATE, CASE WHEN (CUSTOMER.HOMEOWNER_PROJ_DOCUMENT_STA_ID IN ('Signed by Partner', 'Signed by SunEdison')) THEN TO_CHAR(CUSTOMER.PROJ_DEFINITION_DOC_LAST_UPDA, 'IYYY') || '_' || TO_CHAR(CUSTOMER.PROJ_DEFINITION_DOC_LAST_UPDA, 'IW') ELSE NULL END AS PDA_COMPLETE_WW, CASE WHEN (CUSTOMER.HOMEOWNER_PROJ_DOCUMENT_STA_ID IN ('Signed by Partner', 'Signed by SunEdison')) THEN TO_CHAR(CUSTOMER.PROJ_DEFINITION_DOC_LAST_UPDA, 'YYYY') || '_Q' || TO_CHAR(CUSTOMER.PROJ_DEFINITION_DOC_LAST_UPDA, 'Q') ELSE NULL END AS PDA_COMPLETE_Q, NVL2(SITE.COMMISSIONED_DATE, TO_CHAR(SITE.COMMISSIONED_DATE, 'IYYY') || '_' || TO_CHAR(SITE.COMMISSIONED_DATE, 'IW'), NULL) AS COMMISSIONED_WW, NVL2(SITE.COMMISSIONED_DATE, TO_CHAR(SITE.COMMISSIONED_DATE, 'YYYY') || '_Q' || TO_CHAR(SITE.COMMISSIONED_DATE, 'Q'), NULL) AS COMMISSIONED_Q, NVL(SITE.MILESTONE_2_PAYMENT_APPROVAL_,SITE.INSTALLER2_PAYMENT_APPROVAL_D) AS MP2_APPRVD_DATE, NVL2(NVL(SITE.MILESTONE_2_PAYMENT_APPROVAL_,SITE.INSTALLER2_PAYMENT_APPROVAL_D), TO_CHAR(NVL(SITE.MILESTONE_2_PAYMENT_APPROVAL_,SITE.INSTALLER2_PAYMENT_APPROVAL_D), 'IYYY') || '_' || TO_CHAR(NVL(SITE.MILESTONE_2_PAYMENT_APPROVAL_,SITE.INSTALLER2_PAYMENT_APPROVAL_D), 'IW'), NULL) AS MP2_APPRVD_WW, NVL2(NVL(SITE.MILESTONE_2_PAYMENT_APPROVAL_,SITE.INSTALLER2_PAYMENT_APPROVAL_D), TO_CHAR(NVL(SITE.MILESTONE_2_PAYMENT_APPROVAL_,SITE.INSTALLER2_PAYMENT_APPROVAL_D), 'YYYY') || '_Q' || TO_CHAR(NVL(SITE.MILESTONE_2_PAYMENT_APPROVAL_,SITE.INSTALLER2_PAYMENT_APPROVAL_D), 'Q'), NULL) AS MP2_APPRVD_Q, NVL(SITE.MILESTONE_3_PAYMENT_APPROVAL_, SITE.INSTALLER3_PAYMENT_APPROVAL_D) AS MP3_APPRVD_DATE, NVL2(NVL(SITE.MILESTONE_3_PAYMENT_APPROVAL_, SITE.INSTALLER3_PAYMENT_APPROVAL_D), TO_CHAR(NVL(SITE.MILESTONE_3_PAYMENT_APPROVAL_, SITE.INSTALLER3_PAYMENT_APPROVAL_D), 'IYYY') || '_' || TO_CHAR(NVL(SITE.MILESTONE_3_PAYMENT_APPROVAL_, SITE.INSTALLER3_PAYMENT_APPROVAL_D), 'IW'), NULL) AS MP3_APPRVD_WW, NVL2(NVL(SITE.MILESTONE_3_PAYMENT_APPROVAL_, SITE.INSTALLER3_PAYMENT_APPROVAL_D), TO_CHAR(NVL(SITE.MILESTONE_3_PAYMENT_APPROVAL_, SITE.INSTALLER3_PAYMENT_APPROVAL_D), 'YYYY') || '_Q' || TO_CHAR(NVL(SITE.MILESTONE_3_PAYMENT_APPROVAL_, SITE.INSTALLER3_PAYMENT_APPROVAL_D), 'Q'), NULL) AS MP3_APPRVD_Q,NVL2(CUSTOMER.EXECUTED_ASSIGN_AGRMNT_UPDAT, TO_CHAR(CUSTOMER.EXECUTED_ASSIGN_AGRMNT_UPDAT, 'IYYY') || '_' || TO_CHAR(CUSTOMER.EXECUTED_ASSIGN_AGRMNT_UPDAT, 'IW'), NULL) AS ASSIGNMENT_AGMNT_WW, NVL2(CUSTOMER.EXECUTED_ASSIGN_AGRMNT_UPDAT, TO_CHAR(CUSTOMER.EXECUTED_ASSIGN_AGRMNT_UPDAT, 'YYYY') || '_Q' || TO_CHAR(CUSTOMER.EXECUTED_ASSIGN_AGRMNT_UPDAT, 'Q'), NULL) AS ASSIGNMENT_AGMNT_Q, CASE WHEN (customer.financing_program LIKE 'PP%' AND customer.purchase_type_id LIKE 'Leas%') THEN 'AZLS' ELSE customer.financing_program END AS financing_program FROM customer inner join site on customer.site_id = site.record left join sales_order on NVL((CASE when len(site.nr_interconnected_so) != 0 and len(site.nr_interconnected_so) !=1 then substring(site.nr_interconnected_so, 8, (LEN(site.nr_interconnected_so)-7)) end),(CASE when len(site.salesorder_to_homeowner_id) != 0 then substring(site.salesorder_to_homeowner_id, 8, (LEN(site.salesorder_to_homeowner_id)-7)) end)) = sales_order.number";
