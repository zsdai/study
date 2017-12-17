package com.demo.web;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class BaseController {
	
//	@RequestMapping(value = "/")
//  public String index() {
//      return "layout";
//  }
	
	@GetMapping(value = "/")
	public String index() {
		return "index";
	}
}
