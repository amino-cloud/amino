package com._42six.amino.common.query.requests.auditing;

import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;

/**
 * Audit request that is used for AuditorServiceInt.doAudit(). See 'set'
 * methods for required parameters.
 */
public class AminoAuditRequest  {
	
	private String visibility;
	private Boolean collateralSharing = false;
	private String criteria;
	private Calendar dateFrom;
	private Calendar dateTo;
	private String ipAddress;
	private String justification;
	private Boolean logOnly;
	private String numOfSelectors;
	private Collection<String> selectors;
	private String dn;
	private String systemTo;
	/**
	 * @return the visibility
	 */
	public String getVisibility() {
		return visibility;
	}
	/**
	 * Not required.
	 * @param visibility the visibility of the query
	 */
	public void setVisibility(String visibility) {
		this.visibility = visibility;
	}
	/**
	 * @return the collateralSharing
	 */
	public Boolean isCollateralSharing() {
		return collateralSharing;
	}
	/**
	 * Not required. This defaults to false.
	 * @param collateralSharing the collateralSharing to set. Indicates whether a query
	 * can be shared with others.
	 */
	public void setCollateralSharing(Boolean collateralSharing) {
		this.collateralSharing = collateralSharing;
	}
	/**
	 * @return the criteria
	 */
	public String getCriteria() {
		return criteria;
	}
	/**
	 * Audit request must include at least one of: [selectors, dateFrom, dateTo, criteria].
	 * @param criteria the criteria to set. Criteria contains all info about a query not 
	 * already accounted for. For example:
	 * 		queryName=my query name.
	 * 		someAttributes=some other stuff about the query.
	 */
	public void setCriteria(String criteria) {
		this.criteria = criteria;
	}
	/**
	 * @return the dateFrom
	 */
	public Calendar getDateFrom() {
		return dateFrom;
	}
	/**
	 * Only required if date range was used.
	 * Audit request must include at least one of: [selectors, dateFrom, dateTo, criteria].
	 * @param dateFrom the dateFrom to set
	 */
	public void setDateFrom(Calendar dateFrom) {
		this.dateFrom = dateFrom;
	}
	/**
	 * @return the dateTo
	 */
	public Calendar getDateTo() {
		return dateTo;
	}
	/**
	 * Only required if date range was used.
	 * Audit request must include at least one of: [selectors, dateFrom, dateTo, criteria].
	 * @param dateTo the dateTo to set
	 */
	public void setDateTo(Calendar dateTo) {
		this.dateTo = dateTo;
	}
	/**
	 * @return the ipAddress
	 */
	public String getIpAddress() {
		return ipAddress;
	}
	/**
	 * Not required.
	 * @param ipAddress the ipAddress to set
	 */
	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}
	/**
	 * @return the justification
	 */
	public String getJustification() {
		return justification;
	}
	/**
	 * Required.
	 * @param justification the justification to set
	 */
	public void setJustification(String justification) {
		this.justification = justification;
	}
	/**
	 * @return the logOnly
	 */
	public Boolean isLogOnly() {
		return logOnly;
	}
	/**
	 * @return the numOfSelectors
	 */
	public String getNumOfSelectors() {
		return numOfSelectors;
	}
	/**
	 * Not required. This is automatically calculated when setSelectors() or
	 * addSelector() is used.
	 * @param numOfSelectors the numOfSelectors to set
	 */
	public void setNumOfSelectors(String numOfSelectors) {
		this.numOfSelectors = numOfSelectors;
	}
	/**
	 * Required. Depends on the data source.
	 * @param logOnly the logOnly to set
	 */
	public void setLogOnly(Boolean logOnly) {
		this.logOnly = logOnly;
	}
	/**
	 * @return the selectors
	 */
	public Collection<String> getSelectors() {
		return selectors;
	}
	/**
	 * Audit request must include at least one of: [selectors, dateFrom, dateTo, criteria].
	 * This method will update numOfSelectors as well.
	 * @param selectors the selectors to set. 
	 */
	public void setSelectors(Collection<String> selectors) {
		this.selectors = selectors;
		if (selectors != null) {
			numOfSelectors = String.valueOf(selectors.size());
		}
		else {
			numOfSelectors = "0";
		}
	}
	/**
	 * Add a selector to the selector collection. This method will update
	 * numOfSelectors as well.
	 * @param selector
	 */
	public void addSelector(String selector) {
		if (selectors == null) {
			selectors = new HashSet<String>();
		}
		selectors.add(selector);
		numOfSelectors = String.valueOf(selectors.size());
	}

	/**
	 * @return the dn
	 */
	public String getDn() {
		return dn;
	}
	/**
	 * Required.
	 * @param dn the dn to set
	 */
	public void setDn(String dn) {
		this.dn = dn;
	}
	/**
	 * @return the systemTo
	 */
	public String getSystemTo() {
		return systemTo;
	}
	/**
	 * Required.
	 * @param systemTo the systemTo to set.
	 */
	public void setSystemTo(String systemTo) {
		this.systemTo = systemTo;
	}
	
	@Override
	public String toString() {
		return "AminoAuditRequest [visibility=" + visibility
				+ ", collateralSharing=" + collateralSharing + ", criteria="
				+ criteria + ", dateFrom=" + dateFrom + ", dateTo=" + dateTo
				+ ", ipAddress=" + ipAddress + ", justification="
				+ justification + ", logOnly=" + logOnly + ", numOfSelectors="
				+ numOfSelectors + ", selectors=" + selectors + ", dn=" + dn
				+ ", systemTo=" + systemTo + "]";
	}
}
