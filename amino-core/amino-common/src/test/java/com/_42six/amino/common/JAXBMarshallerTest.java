package com._42six.amino.common;

import com._42six.amino.common.service.JAXBMarshaller;
import junit.framework.Assert;
import org.junit.Test;

import javax.xml.bind.JAXBException;
import javax.xml.bind.annotation.XmlRootElement;

public class JAXBMarshallerTest {

	@Test
	public void testMarshal() throws JAXBException, InstantiationException, IllegalAccessException {
		TestBean inputBean = createTestBean();
		
		JAXBMarshaller marshaller = new JAXBMarshaller();
		String beanString = marshaller.marshalToString(inputBean);
		
		TestBean outputBean = marshaller.unmarshal(beanString, TestBean.class);
		Assert.assertEquals(inputBean, outputBean);
	}
	
	private TestBean createTestBean() {
		TestBean bean = new TestBean();
		bean.param1 = "test param 1";
		bean.param2 = "test param 2";
		return bean;
	}
	
	@XmlRootElement
	public static class TestBean {
		public String param1;
		public String param2;

		/* (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((param1 == null) ? 0 : param1.hashCode());
			result = prime * result
					+ ((param2 == null) ? 0 : param2.hashCode());
			return result;
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			TestBean other = (TestBean) obj;
			if (param1 == null) {
				if (other.param1 != null)
					return false;
			} else if (!param1.equals(other.param1))
				return false;
			if (param2 == null) {
				if (other.param2 != null)
					return false;
			} else if (!param2.equals(other.param2))
				return false;
			return true;
		}
	}
}
