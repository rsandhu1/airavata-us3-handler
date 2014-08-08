package org.apache.airavata.gfac.us3.handler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.apache.airavata.commons.gfac.type.ActualParameter;
import org.apache.airavata.gfac.GFacException;
import org.apache.airavata.gfac.core.context.JobExecutionContext;
import org.apache.airavata.gfac.core.context.MessageContext;
import org.apache.airavata.gfac.core.handler.AbstractRecoverableHandler;
import org.apache.airavata.gfac.core.handler.GFacHandler;
import org.apache.airavata.gfac.core.handler.GFacHandlerException;
import org.apache.airavata.gfac.core.utils.GFacUtils;
import org.apache.airavata.gfac.us3.util.US3JobStatus;
import org.apache.airavata.model.workspace.experiment.CorrectiveAction;
import org.apache.airavata.model.workspace.experiment.DataTransferDetails;
import org.apache.airavata.model.workspace.experiment.ErrorCategory;
import org.apache.airavata.model.workspace.experiment.TransferState;
import org.apache.airavata.model.workspace.experiment.TransferStatus;
import org.apache.airavata.registry.api.workflow.ApplicationJob.ApplicationJobStatus;
import org.apache.airavata.registry.cpi.ChildDataType;
import org.apache.airavata.schemas.gfac.ApplicationDeploymentDescriptionType;
import org.apache.airavata.schemas.gfac.StringParameterType;
import org.apache.airavata.schemas.gfac.URIParameterType;
import org.apache.airavata.schemas.wec.ContextHeaderDocument;
import org.apache.airavata.schemas.wec.NameValuePairType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class US3DatabaseOutputHandler extends AbstractRecoverableHandler {

	private static final Logger log = LoggerFactory.getLogger(US3DatabaseOutputHandler.class);
	private static String SELECTDATA = "Select * from analysis where gfacID = ?";
	private static String UPDATEDATA = "UPDATE analysis set stdout =? , stderr = ?, tarfile = ?, status = ? where gfacID = ?";
	private String _experimentStatus = US3JobStatus.COMPLETE.toString();
	public static String INSERTDATA = "INSERT INTO analysis (gfacID, stdout, stderr, tarfile, status, cluster, us3_db) VALUES (?,?,?,?,?,?,?) ";

	Connection connection = null;

	@Override
	public void initProperties(Properties properties) throws GFacHandlerException {
		try {
			String jdbcUrl = properties.getProperty("jdbcUrl");
			String jdbcDriver = properties.getProperty("jdbcDriver");
			Class.forName(jdbcDriver);
			connection = DriverManager.getConnection(jdbcUrl);
		} catch (ClassNotFoundException e) {
			throw new GFacHandlerException(e.getLocalizedMessage(), e);
		} catch (SQLException e) {
			throw new GFacHandlerException(e.getLocalizedMessage(), e);
		}

	}

	@Override
	public void invoke(JobExecutionContext jobExecutionContext) throws GFacHandlerException {
		PreparedStatement statement = null;
		PreparedStatement statement1 = null;
		ResultSet results = null;
		FileInputStream tarData = null;
		File tarFile = null;
		ApplicationDeploymentDescriptionType app = jobExecutionContext.getApplicationContext().getApplicationDeploymentDescription().getType();
		String stdOutput = app.getStandardOutput();
		String stdError = app.getStandardError();
		Map<String, Object> parameters = jobExecutionContext.getOutMessageContext().getParameters();
		String experimentId = jobExecutionContext.getExperimentID();
		try {
			if (jobExecutionContext.getStatus() != null && jobExecutionContext.getStatus().equalsIgnoreCase("CANCELED")) {
				setExperimentStatus(US3JobStatus.CANCELED.toString());
			} else {
				for (String paramName : parameters.keySet()) {
					ActualParameter actualParameter = (ActualParameter) parameters.get(paramName);
					if ("URI".equals(actualParameter.getType().getType().toString())) {
						String outputPath = ((URIParameterType) actualParameter.getType()).getValue();
						if (outputPath != null && !outputPath.isEmpty()) {
							int mid = outputPath.lastIndexOf(".");
							String extension = outputPath.substring(mid + 1, outputPath.length());
							if (!extension.equalsIgnoreCase("tar")) {
								log.info("Experiment status is failed as tar file not created");
								setExperimentStatus(US3JobStatus.FAILED.toString());
							} else {
								log.info(outputPath);
								tarFile = new File(outputPath);
								tarData = new FileInputStream(tarFile);
							}
						} else {
							setExperimentStatus(US3JobStatus.FAILED.toString());
						}
					}
					if ("String".equals(actualParameter.getType().getType().toString())) {
						String outputPath = ((StringParameterType) actualParameter.getType()).getValue();
						if (outputPath != null && !outputPath.isEmpty()) {
							int mid = outputPath.lastIndexOf(".");
							String extension = outputPath.substring(mid + 1, outputPath.length());
							if (!extension.equalsIgnoreCase("tar")) {
								log.info("Experiment status is failed as tar file not created");
								setExperimentStatus(US3JobStatus.FAILED.toString());
							} else {
								log.info(outputPath);
								tarFile = new File(outputPath);
								tarData = new FileInputStream(tarFile);
							}
						} else {
							setExperimentStatus(US3JobStatus.FAILED.toString());
						}
					}
				}
			}
			statement = connection.prepareStatement(SELECTDATA);
			statement.setString(1, experimentId);
			results = statement.executeQuery();

			if (results.next()) {
				statement1 = connection.prepareStatement(UPDATEDATA);
				statement1.setString(1, stdOutput);
				statement1.setString(2, stdError);
				if (tarFile != null && tarFile.length() > 0)
					statement1.setBinaryStream(3, tarData, tarFile.length());
				else
					statement1.setBinaryStream(3, null);
				statement1.setString(4, getExperimentStatus());
				statement1.setString(5, experimentId);
				statement1.executeUpdate();
				log.info("Registered the output data record in LIMS DB for " + experimentId);
			} else {
				statement1 = connection.prepareStatement(INSERTDATA);
				statement1.setString(1, experimentId);
				statement1.setString(2, stdOutput);
				statement1.setString(3, stdError);
				if (tarFile != null && tarFile.length() > 0)
					statement1.setBinaryStream(4, tarData, tarFile.length());
				else
					statement1.setBinaryStream(4, null);
				statement1.setString(5, getExperimentStatus());
				// if(nameValuePairType != null){
				// for (NameValuePairType nameValuePairType2 :
				// nameValuePairType) {
				// String paramName = nameValuePairType2.getName();
				// if(null == paramName){
				// continue;
				// }
				// if(paramName.equalsIgnoreCase("cluster")){
				// statement1.setString(6, nameValuePairType2.getValue());
				// }else if (paramName.equalsIgnoreCase("us3_db")){
				// statement1.setString(7, nameValuePairType2.getValue());
				// }
				// }
				// }else{
				statement1.setString(6, "");
				statement1.setString(7, "");
				// }
				statement1.executeUpdate();
				log.info("Inserted the new output data record in LIMS DB for " + experimentId);
			}

		} catch (SQLException e) {
			log.error("Failed to register output data for " + experimentId + " Error: " + e.getLocalizedMessage());
			throw new GFacHandlerException("Error updating output data to LIMS DB :" + e.getLocalizedMessage() + " Data is availabe at : "
					+ tarFile.getAbsolutePath() + " on gateway server", e);
		} catch (FileNotFoundException e) {
			log.error("Failed to register output data for " + experimentId + " Error: " + e.getLocalizedMessage());
			throw new GFacHandlerException("Error updating output data to LIMS DB :" + e.getLocalizedMessage(), e);

		} catch (NullPointerException e) {
			throw new GFacHandlerException("Application invocation is not successful :" + this.getClass().getName(), e);
		} catch (Exception e) {
			throw new GFacHandlerException("Error running the Handler:" + this.getClass().getName(), e);
		} finally {
			if (_experimentStatus != US3JobStatus.FAILED.toString()) {
				_experimentStatus = US3JobStatus.COMPLETE.toString();
			}
			try {
				if (results != null) {
					results.close();
				}
				if (statement1 != null) {
					statement1.close();
				}
				if (statement != null) {
					statement.close();
				}
				if (connection != null) {
					connection.close();
				}
				if (_experimentStatus == US3JobStatus.FAILED.toString()) {
					ApplicationJobStatus applicationJobStatus = ApplicationJobStatus.FAILED;
					if (null != jobExecutionContext.getJobDetails().getJobID() && !jobExecutionContext.getJobDetails().getJobID().isEmpty()) {
						String id = jobExecutionContext.getJobDetails().getJobID();
						String jobStatusMessage = "Status of job " + id + "is " + applicationJobStatus;
						DataTransferDetails detail = new DataTransferDetails();
						TransferStatus status = new TransferStatus();

						status.setTransferState(TransferState.FAILED);
						detail.setTransferStatus(status);
						try {
							registry.add(ChildDataType.DATA_TRANSFER_DETAIL, detail, jobExecutionContext.getTaskData().getTaskID());
							GFacUtils.saveErrorDetails(jobExecutionContext, "Output is not generated properly", CorrectiveAction.CONTACT_SUPPORT,
									ErrorCategory.FILE_SYSTEM_FAILURE);
						} catch (Exception e1) {
							throw new GFacHandlerException("Error persisting status", e1, e1.getLocalizedMessage());
						}
					}

					throw new GFacHandlerException("Output is not generated properly : Empty Output");
				}
			} catch (SQLException e) {
				throw new GFacHandlerException("Erorr closing connections : " + e.getLocalizedMessage(), e);
			}
		}

	}

	public String getExperimentStatus() {
		return _experimentStatus;
	}

	public void setExperimentStatus(String _experimentStatus) {
		this._experimentStatus = _experimentStatus;
	}

	@Override
	public void recover(JobExecutionContext jobExecutionContext) throws GFacHandlerException {
		// TODO Auto-generated method stub

	}

}