package com._42six.amino.common.service;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

public class JAXBMarshaller {

	/**
	 * Marshals a jaxb annotated object to an xml outputStream 
	 * @param jaxbObject
	 * @return
	 * @throws JAXBException
	 */
	public OutputStream marshal(Object jaxbObject) throws JAXBException {
		final JAXBContext jc = JAXBContext.newInstance(jaxbObject.getClass());
		final Marshaller m = jc.createMarshaller();
		final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

		//Marshal object into file.
		m.marshal(jaxbObject, outputStream);
		return outputStream;
	}
	
	/**
	 * Marshals a jaxb annotated object to an xml string 
	 * @param jaxbObject
	 * @return
	 * @throws JAXBException
	 */
	public String marshalToString(Object jaxbObject) throws JAXBException {
		return marshal(jaxbObject).toString();
	}
	
	/**
	 * Converts xml inputstream to a JAXB object instance
	 * @param inputStream xml inputstream
	 * @param returnClass class object type to return
	 * @return
	 * @throws JAXBException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	@SuppressWarnings("unchecked")
	public <T> T unmarshal(InputStream inputStream, Class<T> returnClass) throws  JAXBException {
		final JAXBContext jc = JAXBContext.newInstance(returnClass);
		final Unmarshaller um = jc.createUnmarshaller();
		
		// Un-marshal XML contents of inputStream into your Java object instance.
		return (T) um.unmarshal(inputStream);
	}
	
	/**
	 * 
	 * Converts xml inputString to a JAXB object instance
	 * @param inputString xml inputString
	 * @param returnClass class object type to return
	 * @return
	 * @throws JAXBException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public <T> T unmarshal(String inputString, Class<T> returnClass) 
			throws JAXBException, InstantiationException, IllegalAccessException {
		return unmarshal(new ByteArrayInputStream(inputString.getBytes()), returnClass);
	}
}