/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.commands.benchmark;

import java.util.Date;

/**
 * @Title: ItemOnlineDetailDO.java
 * @Package com.taobao.matrix.ju.dal.dataobject
 * @Description:
 * @author chenhao.pt
 * @date 2010-7-27 ����07:40:48
 * @version V1.0
 */
/**
 * @ClassName: ItemOnlineDetailDO
 * @Description: ��Ʒ�ϼ���ϸ
 * @author chenhao.pt
 * @date 2010-7-27 ����07:40:48
 *
 */
public class ItemOnlineDetailDO extends BaseDO {

	/**
	* @Fields serialVersionUID :
	*/
	private static final long serialVersionUID = -6915487727855375973L;

	/**
	 * ����������
	 */
	public Integer TASK_STATE_DONE = 1;

	/**
	 * û����������
	 */
	public Integer TASK_STATE_NOT_DONE = 0;
	/**
	 * ����id
	 */
	private Long itemOnlineDetailId;
	/**
	 * ��Ʒid
	 */
	private Long itemId;
	/**
	 * �ϼܿ�ʼʱ��
	 */
	private Date onlineStartTime;
	/**
	 * �ϼܽ���ʱ��
	 */
	private Date onlineEndTime;

	/**
	 * ��Ʒ����
	 */
	private Integer itemCount;
	/**
	 * ��Ʒ����
	 */
	private Integer itemOrder;

	/**
	 * �Ź���
	 */
	private Integer groupNum;

	/**
	 * ����״̬����ʾ��û����������
	 */
	private Integer taskState;

	/**
	 * ʧЧʱ��
	 */
	private Long validOrderTime;

	/**
	 * �ٷֱ�
	 */
	private Double onlinePercent;

	//add by shangu ��ӡ�����Ź�������Ƿ����ƹ�������һ���޹�������3���ֶζ�Ӧ������
	/**
	 * ����Ź�����
	 */
	private Integer lowNum;

	/**
	 * �Ƿ����ƹ������(0:�����ƣ�1:����,2����ɱ)
	 */
	private Integer isLimit = 0;

	/**
	 * һ���޹�����
	 */
	private Integer limitNum = 1;

	private String secKillUrl;

	private String attributes;

	public String getAttributes() {
		return attributes;
	}
	public void setAttributes(String attributes) {
		this.attributes = attributes;
	}

    /**
	 * @return onlinePercent
	 */
	public Double getOnlinePercent() {
		return onlinePercent;
	}
	/**
	 * @param onlinePercent the onlinePercent to set
	 */
	public void setOnlinePercent(Double onlinePercent) {
		this.onlinePercent = onlinePercent;
	}
	/**
	 * @return validOrderTime
	 */
	public Long getValidOrderTime() {
		return validOrderTime;
	}
	/**
	 * @param validOrderTime the validOrderTime to set
	 */
	public void setValidOrderTime(Long validOrderTime) {
		this.validOrderTime = validOrderTime;
	}
	/**
	 * @return taskState
	 */
	public Integer getTaskState() {
		return taskState;
	}
	/**
	 * @param taskState the taskState to set
	 */
	public void setTaskState(Integer taskState) {
		this.taskState = taskState;
	}
	/**
	 * @return itemOnlineDetailId
	 */
	public Long getItemOnlineDetailId() {
		return itemOnlineDetailId;
	}
	/**
	 * @param itemOnlineDetailId the itemOnlineDetailId to set
	 */
	public void setItemOnlineDetailId(Long itemOnlineDetailId) {
		this.itemOnlineDetailId = itemOnlineDetailId;
	}
	/**
	 * @return itemId
	 */
	public Long getItemId() {
		return itemId;
	}
	/**
	 * @param itemId the itemId to set
	 */
	public void setItemId(Long itemId) {
		this.itemId = itemId;
	}
	/**
	 * @return onlineStartTime
	 */
	public Date getOnlineStartTime() {
		return onlineStartTime;
	}
	/**
	 * @param onlineStartTime the onlineStartTime to set
	 */
	public void setOnlineStartTime(Date onlineStartTime) {
		this.onlineStartTime = onlineStartTime;
	}
	/**
	 * @return onlineEndTime
	 */
	public Date getOnlineEndTime() {
		return onlineEndTime;
	}
	/**
	 * @param onlineEndTime the onlineEndTime to set
	 */
	public void setOnlineEndTime(Date onlineEndTime) {
		this.onlineEndTime = onlineEndTime;
	}

	/**
	 * @return itemCount
	 */
	public Integer getItemCount() {
		return itemCount;
	}
	/**
	 * @param itemCount the itemCount to set
	 */
	public void setItemCount(Integer itemCount) {
		this.itemCount = itemCount;
	}
	/**
	 * @return itemOrder
	 */
	public Integer getItemOrder() {
		return itemOrder;
	}
	/**
	 * @param itemOrder the itemOrder to set
	 */
	public void setItemOrder(Integer itemOrder) {
		this.itemOrder = itemOrder;
	}

	/**
	 * @return groupNum
	 */
	public Integer getGroupNum() {
		return groupNum;
	}
	/**
	 * @param groupNum the groupNum to set
	 */
	public void setGroupNum(Integer groupNum) {
		this.groupNum = groupNum;
	}

	public Integer getLowNum() {
		return lowNum;
	}

	public void setLowNum(Integer lowNum) {
		this.lowNum = lowNum;
	}

	public Integer getIsLimit() {
		return isLimit;
	}

	public void setIsLimit(Integer isLimit) {
		this.isLimit = isLimit;
	}

	public Integer getLimitNum() {
		return limitNum;
	}

	public void setLimitNum(Integer limitNum) {
		this.limitNum = limitNum;
	}
	public String getSecKillUrl() {
		return secKillUrl;
	}
	public void setSecKillUrl(String secKillUrl) {
		this.secKillUrl = secKillUrl;
	}

}

