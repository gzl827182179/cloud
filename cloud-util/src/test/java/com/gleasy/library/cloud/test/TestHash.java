package com.gleasy.library.cloud.test;

import java.util.Collections;   
import java.util.HashMap;   
import java.util.Hashtable;   
import java.util.Map;   
import java.util.concurrent.ConcurrentHashMap;   

import com.gleasy.util.Util;
  
/**  
* 测试HashMap和ConcurrentHashMap的并发性能差别。  
*   
* @author 老紫竹 JAVA世纪网(java2000.net)  
*   
*/  
public class TestHash {   
  static final int threads = 500;   
  static final int NUMBER = 10000;   
  public static String key = "";
  
  public static void main(String[] args) throws Exception {   
    Map<String, Integer> hashmapSync = new HashMap<String, Integer>();   
    Map<String, Integer> concurrentHashMap = new ConcurrentHashMap<String, Integer>();   
    Map<String, Integer> hashtable = new Hashtable<String, Integer>();   
  
    long totalA = 0;   
    long totalB = 0;   
    long totalC = 0;   
//    for (int i = 0; i <= 10; i++) {   
//      totalA += testPut(hashmapSync);   
//      totalB += testPut(concurrentHashMap);   
//      totalC += testPut(hashtable);   
//    }   
//    System.out.println("Put time HashMapSync=" + totalA + "ms.");   
//    System.out.println("Put time ConcurrentHashMap="+ totalB + "ms.");   
//    System.out.println("Put time Hashtable=" + totalC + "ms.");   
//  
    totalA = 0;   
    totalB = 0;   
    totalC = 0;   
    TestHash.key = Util.genUniqueId("dd");
    for (int i = 0; i <= 10; i++) {   
      //totalA += testGet(hashmapSync);   
      totalB += testGet(concurrentHashMap);   
      //totalC += testGet(hashtable);   
    }   
    System.out.println("Get time HashMapSync=" + totalA + "ms.");   
    System.out.println("Get time ConcurrentHashMap="+ totalB + "ms.");   
    System.out.println("Get time Hashtable=" + totalC + "ms.");   
  }   
  
  public static long testPut(Map<String, Integer> map) throws Exception {   
    long start = System.currentTimeMillis();   
    for (int i = 0; i < threads; i++) {   
      new MapPutThread(map).start();   
    }   
    while (MapPutThread.counter > 0) {   
      Thread.sleep(1);   
    }   
    return System.currentTimeMillis() - start;   
  }   
  
  public static long testGet(Map<String, Integer> map) throws Exception {   
    long start = System.currentTimeMillis();   
    for (int i = 0; i < threads; i++) {   
      new MapGetThread(map).start();  
      new MapPutThread(map).start();
    }   
    while (MapGetThread.counter > 0 || MapPutThread.counter > 0) {   
      Thread.sleep(1);   
    }   
    return System.currentTimeMillis() - start;   
  }   
}   
  
class MapPutThread extends Thread {   
  static int counter = 0;   
  static Object lock = new Object();   
  private Map<String, Integer> map;   

  MapPutThread(Map<String, Integer> map) {   
    synchronized (lock) {   
      counter++;   
    }   
    this.map = map;   
  }   
  
  public void run() {   
  
    for (int i = 1; i <= TestHash.NUMBER; i++) {   
      map.put(TestHash.key+":"+(int)Math.round(Math.random()*TestHash.NUMBER), i);   
    }   
    synchronized (lock) {   
      counter--;   
    }   
  }   
}   
  
class MapGetThread extends Thread {   
  static int counter = 0;   
  static Object lock = new Object();   
  private Map<String, Integer> map;   
  private String key = this.getId() + "";   
  
  MapGetThread(Map<String, Integer> map) {   
    synchronized (lock) {   
      counter++;   
    }   
    this.map = map;   
    key = Util.genUniqueId("ss");
  }   
  
  public void run() {   
  
    for (int i = 1; i <= TestHash.NUMBER; i++) {  
      map.get(key+":"+(int)Math.round(Math.random()*TestHash.NUMBER));   
    }   
    synchronized (lock) {   
      counter--;   
    }   
  }   
} 
