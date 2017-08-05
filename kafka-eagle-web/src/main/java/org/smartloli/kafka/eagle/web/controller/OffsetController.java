/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartloli.kafka.eagle.web.controller;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.MetadataResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.ModelAndView;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import org.smartloli.kafka.eagle.common.util.Constants;
import org.smartloli.kafka.eagle.common.util.SystemConfigUtils;
import org.smartloli.kafka.eagle.web.service.OffsetService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Kafka offset controller to viewer data.
 * 
 * @author smartloli.
 *
 *         Created by Sep 6, 2016.
 * 
 *         Update by hexiang 20170216
 */
@Controller
public class OffsetController {
private Logger LOG = LoggerFactory.getLogger(OffsetController.class);
	/** Offsets consumer data interface. */
	@Autowired
	private OffsetService offsetService;

	/** Consumer viewer. */
	@RequestMapping(value = "/consumers/offset/{group}/{topic}/", method = RequestMethod.GET)
	public ModelAndView consumersActiveView(@PathVariable("group") String group, @PathVariable("topic") String topic, HttpServletRequest request) {
		ModelAndView mav = new ModelAndView();
		HttpSession session = request.getSession();
		String clusterAlias = session.getAttribute(Constants.SessionAlias.CLUSTER_ALIAS).toString();
		String formatter = SystemConfigUtils.getProperty("kafka.eagle.offset.storage");
		if (offsetService.hasGroupTopic(clusterAlias, formatter, group.replaceAll("==","/"), topic)) {
			mav.setViewName("/consumers/offset_consumers");
		} else {
			mav.setViewName("/error/404");
		}
		return mav;
	}


	/** Get real-time offset data from Kafka by ajax. */
	@RequestMapping(value = "/consumers/offset/{group}/{topic}/realtime", method = RequestMethod.GET)
	public ModelAndView offsetRealtimeView(@PathVariable("group") String group, @PathVariable("topic") String topic, HttpServletRequest request) {
		ModelAndView mav = new ModelAndView();
		HttpSession session = request.getSession();
		String clusterAlias = session.getAttribute(Constants.SessionAlias.CLUSTER_ALIAS).toString();
		String formatter = SystemConfigUtils.getProperty("kafka.eagle.offset.storage");
		if (offsetService.hasGroupTopic(clusterAlias, formatter, group.replaceAll("==","/"), topic)) {
			mav.setViewName("/consumers/offset_realtime");
		} else {
			mav.setViewName("/error/404");
		}
		return mav;
	}
	public long  offsets(String group,String topic,int partition)  throws IOException {
        LOG.info("n1");
		SendProtocol sp = new SendProtocol();
        LOG.info("n2");
        sp.GroupCoordinator(group.replaceAll("==","/"));
        LOG.info("n3");
		List<String> topics = new ArrayList<String>();
        LOG.info("n4");
		topics.add(topic);
        LOG.info("n5");
		Long offset=(long)-1;
        LOG.info("n6");
		MetadataResponse.TopicMetadata res = sp.Metadata(topics);
        LOG.info("n7");
		Iterator it2 = res.partitionMetadata().iterator();
        LOG.info("n8");
		while (it2.hasNext()) {
        LOG.info("n9");
			MetadataResponse.PartitionMetadata ans = (MetadataResponse.PartitionMetadata) it2.next();
			if (ans.partition() == partition) {
        LOG.info("n10");
				TopicPartition tp = new TopicPartition(topic, ans.partition());
        LOG.info("n11");
				offset=sp.listoffsets(tp, ans.leader().host(), ans.leader().port());
        LOG.info("n12");
                break;
			}
		}
		return offset;
	}
	/** Get detail offset from Kafka by ajax. */
	@RequestMapping(value = "/consumer/offset/{group}/{topic}/ajax", method = RequestMethod.GET)
	public void offsetDetailAjax(@PathVariable("group") String group, @PathVariable("topic") String topic, HttpServletResponse response, HttpServletRequest request) throws IOException {
		String aoData = request.getParameter("aoData");
		JSONArray params = JSON.parseArray(aoData);
		int sEcho = 0, iDisplayStart = 0, iDisplayLength = 0;
		for (Object object : params) {
			JSONObject param = (JSONObject) object;
			if ("sEcho".equals(param.getString("name"))) {
				sEcho = param.getIntValue("value");
			} else if ("iDisplayStart".equals(param.getString("name"))) {
				iDisplayStart = param.getIntValue("value");
			} else if ("iDisplayLength".equals(param.getString("name"))) {
				iDisplayLength = param.getIntValue("value");
			}
		}

		HttpSession session = request.getSession();
		String clusterAlias = session.getAttribute(Constants.SessionAlias.CLUSTER_ALIAS).toString();

		String formatter = SystemConfigUtils.getProperty("kafka.eagle.offset.storage");
		JSONArray logSizes = JSON.parseArray(offsetService.getLogSize(clusterAlias, formatter, topic, group.replaceAll("==","/")));
		int offset = 0;
		JSONArray aaDatas = new JSONArray();
		for (Object object : logSizes) {
			JSONObject logSize = (JSONObject) object;
			if (offset < (iDisplayLength + iDisplayStart) && offset >= iDisplayStart) {
				JSONObject obj = new JSONObject();
				obj.put("partition", logSize.getInteger("partition"));
				if (logSize.getLong("logSize") == 0) {
					obj.put("logsize", "<a class='btn btn-warning btn-xs'>0</a>");
				} else {
					obj.put("logsize", logSize.getLong("logSize"));
				}
				if (logSize.getLong("offset") == -1) {
					obj.put("offset", "<a class='btn btn-warning btn-xs'>0</a>");
				} else {
					obj.put("offset", "<a class='btn btn-success btn-xs'>" + logSize.getLong("offset") + "</a>");
				}
				obj.put("lag", "<a class='btn btn-danger btn-xs'>" + logSize.getLong("lag") + "</a>");
				obj.put("owner", logSize.getString("owner"));
				obj.put("node", logSize.getString("node"));
				obj.put("created", logSize.getString("create"));
				obj.put("modify", logSize.getString("modify"));
				int pa=logSize.getInteger("partition");
                long earOff = offsets(group.replaceAll("==","/"),topic,pa);
				obj.put("earlistoffset",earOff);
				aaDatas.add(obj);
			}
			offset++;
		}

		JSONObject target = new JSONObject();
		target.put("sEcho", sEcho);
		target.put("iTotalRecords", logSizes.size());
		target.put("iTotalDisplayRecords", logSizes.size());
		target.put("aaData", aaDatas);
		try {
			byte[] output = target.toJSONString().getBytes();
			BaseController.response(output, response);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	/** Get real-time offset graph data from Kafka by ajax. */
	@RequestMapping(value = "/consumer/offset/{group}/{topic}/realtime/ajax", method = RequestMethod.GET)
	public void offsetGraphAjax(@PathVariable("group") String group, @PathVariable("topic") String topic, HttpServletResponse response, HttpServletRequest request) {
		HttpSession session = request.getSession();
		String clusterAlias = session.getAttribute(Constants.SessionAlias.CLUSTER_ALIAS).toString();

		try {
			byte[] output = offsetService.getOffsetsGraph(clusterAlias, group.replaceAll("==","/"), topic).getBytes();
			BaseController.response(output, response);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}
