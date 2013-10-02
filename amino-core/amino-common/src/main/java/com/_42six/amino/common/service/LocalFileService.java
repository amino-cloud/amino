package com._42six.amino.common.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import com._42six.amino.common.exception.LocalFileServiceException;

/**
 * Service responsible for interacting with the local file system.
 */
public class LocalFileService {	

	/**
	 * @param filePath
	 * @return true if this file exists
	 */
	public boolean doesFileExist(String filePath) {
		return new File(filePath).exists();
	}
	
	/**
	 * Verifies whether the security manager has write permission to this file
	 * @param filePath
	 * @return true if we have write access
	 */
	public boolean checkWritePermission(String filePath) {
		try {
			System.getSecurityManager().checkWrite(filePath);
			return true;
		}
		catch (Exception e) {
			return false;
		}
	}

	/**
	 * Copies a file.  If the file already exists, it is overwritten. Sets permission
	 * to 775.
	 * @param inputFile file to be copied
	 * @param outputFile output file
	 * @param mkDirs true if parent directories should be automatically created
	 * @throws LocalFileServiceException
	 */
	public void copyFile(File inputFile, File outputFile, boolean mkDirs) throws LocalFileServiceException {
		try {
			if (mkDirs) {
				outputFile.getParentFile().mkdirs();
			}
			
			InputStream in = new FileInputStream(inputFile);

			//For Overwrite the file.
			OutputStream out = new FileOutputStream(outputFile);

			byte[] buf = new byte[1024];
			int len;
			while ((len = in.read(buf)) > 0){
				out.write(buf, 0, len);
			}
			in.close();
			out.close();
			
			//set file permissions so others can read and execute
			outputFile.setExecutable(true, false);
			outputFile.setReadable(true, false);
			outputFile.setWritable(true);
		}
		catch (Exception e) {
			throw new LocalFileServiceException("Unable to copy file [" + inputFile +"] to [" + outputFile + "]", e);
		}
	}
	
	/**
	 * Recursively delete all files and folders in a directory
	 * @param directory
	 */
	public void deleteAllFilesAndFoldersInDirectory(File directory) {
		List<File> fileList = walkFiles(directory.getAbsolutePath(), null, true, true);
		for (File file : fileList) {
			deleteFile(file);
		}
	}

	/**
	 * Delete a file
	 * @param file
	 * @return
	 */
	public boolean deleteFile(File file) {
		return file.delete();
	}

	/**
	 * List files matching the fileNameRegex
	 * @param basePath Base path to start in
	 * @param fileNameRegex File name to match
	 * @param recursive Whether to search folders recursively
	 * @return list of matching files
	 */
	public List<File> walkFiles(String basePath, String fileNameRegex, boolean recursive, boolean alsoListFolders) {
		List<File> fileList = new ArrayList<File>();
		File[] faFiles = new File(basePath).listFiles();
		if (faFiles != null) {
			for(File file: faFiles){
				if(recursive && file.isDirectory()){
					if (alsoListFolders) {
						fileList.add(file);
					}
					fileList.addAll(walkFiles(file.getAbsolutePath(), fileNameRegex, recursive, alsoListFolders));
				}
				else if (fileNameRegex == null || file.getName().matches(fileNameRegex)){
					fileList.add(file);
				}
			}
		}
		return fileList;
	}
}

