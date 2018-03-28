package com.walloce.pvcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.WritableComparable;

public class PageView implements WritableComparable<PageView> {
	
	/**
	 * 访问的visit_id
	 */
	private String visit_id;
	
	/**
	 * 当前访问的URL
	 */
	private String visit_url;
	
	/**
	 * 访问当前页面前的url
	 */
	private String visit_pre_url;
	
	/**
	 * 当前访问的用户的标识
	 */
	private String visit_user_guid;
	
	/**
	 * 访问时session的id，有效时效为最后一次访问后半小时
	 */
	private String visit_session_id;
	
	/**
	 * 访问时用户的ip
	 */
	private String visit_ip;
	
	/**
	 * 省份标识
	 */
	private String province_id;
	
	/**
	 * 城市标识
	 */
	private String city_id;
	
	/**
	 * 使用的浏览器版本
	 */
	private String browser_version;
	
	/**
	 * 访问的系统，例：windows7
	 */
	private String visit_platform;
	
	
	
	public String getVisit_id() {
		return visit_id;
	}

	public void setVisit_id(String visit_id) {
		this.visit_id = visit_id;
	}

	public String getVisit_url() {
		return visit_url;
	}

	public void setVisit_url(String visit_url) {
		this.visit_url = visit_url;
	}

	public String getVisit_pre_url() {
		return visit_pre_url;
	}

	public void setVisit_pre_url(String visit_pre_url) {
		this.visit_pre_url = visit_pre_url;
	}

	public String getVisit_user_guid() {
		return visit_user_guid;
	}

	public void setVisit_user_guid(String visit_user_guid) {
		this.visit_user_guid = visit_user_guid;
	}

	public String getVisit_session_id() {
		return visit_session_id;
	}

	public void setVisit_session_id(String visit_session_id) {
		this.visit_session_id = visit_session_id;
	}

	public String getVisit_ip() {
		return visit_ip;
	}

	public void setVisit_ip(String visit_ip) {
		this.visit_ip = visit_ip;
	}

	public String getProvince_id() {
		return province_id;
	}

	public void setProvince_id(String province_id) {
		this.province_id = province_id;
	}

	public String getCity_id() {
		return city_id;
	}

	public void setCity_id(String city_id) {
		this.city_id = city_id;
	}

	public String getBrowser_version() {
		return browser_version;
	}

	public void setBrowser_version(String browser_version) {
		this.browser_version = browser_version;
	}

	public String getVisit_platform() {
		return visit_platform;
	}

	public void setVisit_platform(String visit_platform) {
		this.visit_platform = visit_platform;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.browser_version);
		out.writeUTF(this.province_id);
		out.writeUTF(this.city_id);
		out.writeUTF(this.visit_id);
		out.writeUTF(this.visit_ip);
		out.writeUTF(this.visit_platform);
		out.writeUTF(this.visit_pre_url);
		out.writeUTF(this.visit_session_id);
		out.writeUTF(this.visit_url);
		out.writeUTF(this.visit_user_guid);
		
	}

	public void readFields(DataInput in) throws IOException {
		this.browser_version = in.readUTF();
		this.city_id = in.readUTF();
		this.province_id = in.readUTF();
		this.visit_id = in.readUTF();
		this.visit_ip = in.readUTF();
		this.visit_platform = in.readUTF();
		this.visit_pre_url = in.readUTF();
		this.visit_session_id = in.readUTF();
		this.visit_url = in.readUTF();
		this.visit_user_guid = in.readUTF();
		
	}

	public int compareTo(PageView o) {
		// TODO Auto-generated method stub
		if (StringUtils.isBlank(this.browser_version) || StringUtils.isBlank(this.visit_platform)) {
			return 1;
		}
		return 0;
	}

	@Override
	public String toString() {
		String resultStr = 
				this.visit_id +"\t"+ this.visit_user_guid +"\t"+
				this.visit_ip +"\t"+ this.province_id +"\t"+
				this.city_id +"\t"+ this.visit_pre_url +"\t"+ 
				this.visit_url +"\t"+ this.visit_session_id +"\t"+ 
				this.visit_platform +"\t"+ this.browser_version;
		return resultStr;
	}

}
