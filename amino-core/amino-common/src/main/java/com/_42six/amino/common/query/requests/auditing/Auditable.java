/**
 * 
 */
package com._42six.amino.common.query.requests.auditing;

/**
 * @author root
 *
 */
public interface Auditable {
	/**
	 * @return The justification for why the {@link com._42six.amino.common.query.requests.Request} was made
	 */
	public String getJustification();
	
	/**
	 * @param justification The justification for why the {@link com._42six.amino.common.query.requests.Request} was made
	 */
	public void setJustifcation(String justification);
}
