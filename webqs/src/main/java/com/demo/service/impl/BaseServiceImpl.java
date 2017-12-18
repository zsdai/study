package com.demo.service.impl;

import java.io.Serializable;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.repository.JpaRepository;

import com.demo.service.BaseService;

public abstract class BaseServiceImpl<T> implements BaseService<T> {

	@Override
	public abstract JpaRepository<T, Serializable> getRepository() ;
	@Override
	public Page<T> getPage(int pageNumber, int pageSize,Sort sort) {
		Page<T> page = this.getRepository().findAll(new PageRequest(pageNumber - 1, pageSize, sort));
		return page;
	}

}
