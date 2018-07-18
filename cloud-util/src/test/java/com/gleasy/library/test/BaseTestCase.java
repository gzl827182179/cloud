package com.gleasy.library.test;

import java.util.ArrayList;
import java.util.List;

import com.gleasy.util.SpringFactory;

import junit.framework.TestCase;

public class BaseTestCase extends TestCase{
    static{
//		List<String> files = new ArrayList<String>();
//		files.add("applicationContext.xml");
//		SpringFactory.setConfigClasses(files);
    }
    
	protected void setUp() { 

	} 

	protected void tearDown() throws Exception {  
	}  
}
