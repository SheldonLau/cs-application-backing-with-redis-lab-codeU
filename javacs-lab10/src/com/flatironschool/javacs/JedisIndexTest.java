/**
 * 
 */
package com.flatironschool.javacs;
import java.util.*;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.io.IOException;
import java.util.Map;

import org.jsoup.select.Elements;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;

/**
 * @author downey
 *
 */
public class JedisIndexTest {

	private static String url1, url2;
	private Jedis jedis;
	private JedisIndex index;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		jedis = JedisMaker.make();
		index = new JedisIndex(jedis);
		
		loadIndex(index);
	}
	
	/**
	 * Loads the index with two pages read from files.
	 * 
	 * @return
	 * @throws IOException
	 */
	private static void loadIndex(JedisIndex index) throws IOException {
		WikiFetcher wf = new WikiFetcher();

		url1 = "https://en.wikipedia.org/wiki/Java_(programming_language)";
		Elements paragraphs = wf.fetchWikipedia(url1);
		index.indexPage(url1, paragraphs);
		
		url2 = "https://en.wikipedia.org/wiki/Programming_language";
		paragraphs = wf.fetchWikipedia(url2);
		index.indexPage(url2, paragraphs);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		jedis.close();
	}

	/**
	 * Test method for {@link com.flatironschool.javacs.JedisIndex#getCounts(java.lang.String)}.
	 */
//  @Test
//  public void testAdd() {
//    Map<String, Integer> map = new 
//  }
	@Test
	public void testGetCounts() {
		Map<String, Integer> map = index.getCounts("the");
//    String value = jedis.hget(index.termCounterKey(url1), "the");
//    assertThat(value, is("339"));
//    assertThat(index.getURLs("the"), hasItem(url1));
//    assertThat(index.getURLs("the"), hasItem(url2));
		assertThat(map.get(url1), is(346));
		assertThat(map.get(url2), is(264));
	}

  @Test
  public void testGetCount() {
		Integer count1 = index.getCount(url1, "the");
		Integer count2 = index.getCount(url2, "the");
		assertThat(count1, is(346));
		assertThat(count2, is(264));
  }

  @Test
  public void testPushTermCounterToRedis() throws Exception{
    
		WikiFetcher wf = new WikiFetcher();
		Elements paragraphs = wf.fetchWikipedia(url1);
    TermCounter tc = new TermCounter(url1);
    tc.processElements(paragraphs);
    List<Object> contents = index.pushTermCounterToRedis(tc);
    assertThat(contents.size(), not(0));
    assertThat(jedis.hget(tc.getLabel(), "the"), is("346"));
  }
}
