package com.hualu.cloud.serverinterface;

import java.io.IOException;
import java.util.TimerTask;
import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;

import com.hualu.cloud.basebase.staticparameter;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

public class mdbMonite extends TimerTask{
	public static Logger logger = Logger.getLogger(mdbMonite.class);
	public static boolean S2=true;//true：需要设定Slave,false:已经设定Slave，
	public static boolean ms1=false;//true:上一周期memorydb1可以连上，false:周期memorydb1连不上
	public static boolean ms2=false;//true:上一周期memorydb1可以连上，false:周期memorydb1连不上
	public static int MemoryDBcluster=staticparameter.getIntValue("MemoryDBcluster", 0);//0非内存数据库集群
	public static String master=staticparameter.getValue("memorydbhostipm", "127.0.0.1");
		
	public static String memorydbhostipm=staticparameter.getValue("memorydbhostipm", "127.0.0.1");
	public static int memorydbportm=staticparameter.getIntValue("memorydbportm",6379);
	public static String memorydbhostips=staticparameter.getValue("memorydbhostips", "127.0.0.1");
	public static int memorydbports=staticparameter.getIntValue("memorydbports",6379);
	
	public static Jedis memorydb1=new Jedis(memorydbhostipm,memorydbportm);
	public static Jedis memorydb2=new Jedis(memorydbhostips,memorydbports);
	
	int CONNECT_SECONDS=50;//重连10次，相当于连接10秒
	
	public void run() {  
		logger.info("进入redis内存数据库定时监控线程monite");
		
		if(MemoryDBcluster==1){
			//切记当memorydb1和memorydb2为static时，isConnected的判断不准确。不要用这个做判断。
			
			logger.info("beforce检测内存数据库连接：记录Master是"+master);
			logger.info("检测链接memorydb1...");
			try{
				String ss=memorydb1.get("Master");
				if(ss==null)
					memorydb1.set("Master",memorydbhostipm );
				else 
					master=ss;		
				logger.info("链接memorydb1成功");
			}catch(Exception e){
					try{
						try{
							memorydb1.quit();
						}catch(Exception e1){
							logger.warn(e1.getMessage());
						}
						memorydb1.disconnect();
						memorydb1.connect();//配置文件中的master
						logger.info("重链memorydb1成功");
					}catch(Exception e1){
						logger.warn("重连内存数据库 "+memorydbhostipm+"出错："+e1.getMessage());
					}
			}
			
			logger.info("链接memorydb2...");
			try{
				String ss=memorydb2.get("Master");
				if(ss==null)
					memorydb1.set("Master",memorydbhostips );
				else 
					master=ss;	
				logger.info("链接memorydb2成功");
			}catch(Exception e){
				try{
					try{
						memorydb1.quit();
					}catch(Exception e1){
						logger.warn(e1.getMessage());
					}
					memorydb2.disconnect();
					memorydb2.connect();//配置文件中的slave
					logger.info("重连memorydb2成功");
				}catch(Exception e1){
					logger.warn("重连链接内存数据库"+memorydbhostips+"出错："+e1.getMessage());
				}
			}
			System.out.println("memorydb1.isConnceted()="+memorydb1.isConnected());
			System.out.println("memorydb2.isConnected()="+memorydb2.isConnected());
			
			logger.info("记录Master是"+master);
	
			if(memorydb1.isConnected()==true){
				if(master.indexOf(memorydbhostipm)>=0){
					logger.debug("1");
					//memorydb1可以连上，上周期Master为memorydb1
					if(ms1==false){
						logger.debug("11");
						//上一周期memorydb1没有连上，则视为本周期连上是重启了服务，因此要重新设定Master
						try{
							memorydb1.slaveofNoOne();
							try{
								memorydb1.set("Master", memorydbhostipm);
								master=memorydbhostipm;
								S2=true;//只要重新设定了Master，就应该重新设定Slave
								logger.info("本周期切换"+memorydbhostipm+"为 Master正常");
								logger.info("本周期切换 Master正常");
							}catch(Exception e1){
								logger.warn("在内存数据库"+memorydbhostipm+"设置master值异常"+e1.getMessage());
							}
						}catch(Exception e){						
							logger.warn("将"+memorydbhostipm+"切换为master异常"+e.getMessage());
							try{
								memorydb1.quit();
							}catch(Exception e2){
								logger.warn(e2.getMessage());
							}
							memorydb1.disconnect();
							memorydb1=new Jedis(memorydbhostipm,memorydbportm);
							try{
								memorydb1.connect();
								logger.info("重连内存数据库"+memorydbhostipm+"成功！");
								memorydb1.slaveofNoOne();
								logger.info("重连后将"+memorydbhostipm+"切换为Master成功！");
								memorydb1.set("Master", memorydbhostipm);
								logger.info("重连后在内存数据库"+memorydbhostipm+"内设置Master值成功！");
								master=memorydbhostipm;
								ms1=true;
								S2=true;//只要重新设定了Master，就应该重新设定Slave
							}catch(Exception e2){
								logger.warn("重连后操作内存数据库"+memorydbhostipm+"异常："+e2.getMessage());
							}
						}
					}else{//ms1==true
						//上一周期memorydb1连上了，且上周期Master为memorydb1，因此不需要重新设定Master
						logger.debug("12");					
						logger.info(memorydbhostipm+"本周期正常，不需要进行master切换");
					}
					//处理slave
					if(S2==true){
						logger.debug("13");
						//需要设定Slave为memorydbhostips
						if(memorydb2.isConnected()==true){
							try{
								memorydb2.slaveof(memorydbhostipm, memorydbportm);
								S2=false;
								logger.info("本周期切换 "+memorydbhostips+" 为 Slave 正常！");
							}catch(Exception e){
								logger.warn("将"+memorydbhostips+"设为"+memorydbhostipm+"的slave异常"+e.getMessage());
							}	
							//设置本周期memorydb2的状态。
							ms2=true;
						}else{
							//设置本周期memorydb2的状态。
							ms2=false;
							logger.warn("本周期需要切换 "+memorydbhostips+" 为Slave，但是该节点连不上，不能进行切换！");
						}
					}else{
						logger.debug("14");
						//处理slave
						if(ms2==false){
							if(memorydb2.isConnected()==true){
								try{
									memorydb2.slaveof(memorydbhostipm, memorydbportm);
									S2=false;
									logger.info("本周期切换 "+memorydbhostips+" 为 Slave 正常！");
								}catch(Exception e){
									logger.warn("将"+memorydbhostips+"设为"+memorydbhostipm+"的slave异常"+e.getMessage());
									S2=true;
								}	
							}
						}
	
						//设置本周期memorydb2的状态
						if(memorydb2.isConnected()==true)
							ms2=true;
						else
							ms2=false;
						logger.info("本周期不需要进行slave切换");
					}
				}
				if(master.indexOf(memorydbhostips)>=0){
					logger.debug("2");
					////memorydb1可以连上，上周期Master为memorydb2
					logger.info("上周期Master为memorydb2...");
					if(memorydb2.isConnected()==true){
						logger.debug("21");
						//memorydb2可以连接，上周期Master为memorydbhostips
						if(ms2==false){
							//上周期memorydb2没连上，是为本周期服务重启了，故要重新设定该节点为Master
							try{
								memorydb2.slaveofNoOne();
							}catch(Exception e){
								logger.warn("将"+memorydbhostips+"设为Master异常");
							}
							try{
								memorydb2.set("Master", memorydbhostips);
								master=memorydbhostips;
								S2=true;
								logger.info("本周期切换 "+memorydbhostips+" 为 Master 正常！");
							}catch(Exception e){
								logger.warn(memorydbhostips+"内存数据库设置Master的值异常");
							}
						}
						if(S2==true){
							//需要设定Slave为memorydbhostipm
							try{
								memorydb1.slaveof(memorydbhostips, memorydbports);
								S2=false;
								logger.info("本周期切换 "+memorydbhostipm+" 为 Slave 正常！");
							}catch(Exception e){
								logger.warn("将"+memorydbhostipm+"设为"+memorydbhostips+"的slave异常"+e.getMessage());
							}
						}
						//设置本周期memorydb1和memorydb2状态
						ms2=true;
						ms1=true;
					}else{
						logger.debug("22");
						//memorydb1可以连上，memorydb2连不上
						//内存中memorydb2为master,故要反复确认memorydb2是否可用
						int i=0,j=0,k=0;
						while(true){
							k++;
					    	try{
						    	try { 
						    		memorydb2.quit(); 
						    		} catch (Exception e) { 
						    			logger.debug(""+memorydbhostips+"内存数据库quit异常："+e.getMessage());
						       		} 
					    		memorydb2.disconnect(); 
					    		} catch (Exception e) { 
					    			logger.debug(""+memorydbhostips+"内存数据库disconnect异常："+e.getMessage());
					    		}
						    	try{
						    		memorydb2.connect();
							    }catch(Exception e){
							    	logger.error("connect内存数据库"+memorydbhostips+"异常"+e.getMessage());
							    }
							if(memorydb2.isConnected()==false)
								j++;
							else
								i++;
							if(k>CONNECT_SECONDS)//测试2分钟内存数据库的链接
								break;
							try {
								Thread.sleep(1000);//测试间隔为1秒
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
						if(j>i){
							//反复确认memorydb2不可用，memorydb1可用，
							//则将memorydb1设为Master,并将S2置为true
							try{
								memorydb1.slaveofNoOne();
								S2=true;
								logger.info("将"+memorydbhostipm+"设为Master正常！");
							}catch(Exception e){
								logger.error("将"+memorydbhostipm+"设为Master异常："+e.getMessage());
						    	try { 
						    		memorydb1.quit(); 
						    	} catch (Exception e1) { 
						    			logger.debug("退出"+memorydbhostipm+"内存数据库quit异常："+e.getMessage());
						       	}
						    	try{
					    			memorydb1.disconnect(); 
					    		}catch(Exception e2) { 
					    			logger.debug(""+memorydbhostipm+"内存数据库disconnect异常："+e2.getMessage());
					    		}
						    	try{
						    		memorydb1.connect();
							    }catch(Exception e3){
							    	logger.error("connect内存数据库"+memorydbhostipm+"异常"+e3.getMessage());
							    }
								try{
									memorydb1.slaveofNoOne();
									S2=true;
									logger.info("将"+memorydbhostipm+"设为Master正常！");
								}catch(Exception e4){
									logger.error("将"+memorydbhostipm+"设为Master异常："+e4.getMessage());
								}
							}
							try{
								master=memorydbhostipm;
								memorydb1.set("Master", memorydbhostipm);
								logger.info("将"+memorydbhostipm+"设为Master OK！");
							}catch(Exception e5){
								logger.warn("在内存数据库"+memorydbhostipm+"中设置Master的值异常："+e5.getMessage());
							}
							//设置本周期memorydb1和memorydb2的状态。
							ms1=true;
							ms2=false;
						}else{
							//反复确认Memorydb2可用，将memorydb2设为master
							logger.debug("23");
							try{
								memorydb2.slaveofNoOne();
								master=memorydbhostips;
								S2=true;
								try{
									memorydb2.set("Master", memorydbhostips);
									logger.info("将"+memorydbhostips+"设为Master OK！");
								}catch(Exception e){
									logger.warn("在内存数据库"+memorydbhostips+"中设置Master值异常："+e.getMessage());
								}
							}catch(Exception e){
								logger.warn("将"+memorydbhostipm+"设为Master异常："+e.getMessage());
							}
	
							//memorydb1可用，所以同时设为slave
							try{
								memorydb1.slaveof(memorydbhostips, memorydbports);
								S2=false;
								logger.info("将"+memorydbhostips+"设为Master OK!");
							}catch(Exception e){
								logger.warn("将"+memorydbhostipm+"设为"+memorydbhostips+"的slave异常"+e.getMessage());
							}
							//设置本周期memorydb1和memorydb2的状态。
							ms1=true;
							ms2=true;
						}
					}
				}
			}else{//memorydb1连不上
				if(memorydb2.isConnected()==true){
					logger.debug("3");
					if(master.indexOf(memorydbhostipm)>=0){
						logger.debug("31");
						//原Master为memorydb1，但memorydb1连不上
						//为减少切换，所以要反复确认memorydb1是否可用
						int i=0,j=0,k=0;
						while(true){
							k++;
					    	try{
						    	try { 
						    		memorydb1.quit(); 
						    		} catch (Exception e) { 
						    			logger.debug(memorydbhostipm+"内存数据库quit异常："+e.getMessage());
						       		} 
					    		memorydb1.disconnect(); 
					    	} catch (Exception e) { 
					    		logger.debug(memorydbhostipm+"内存数据库disconnect异常："+e.getMessage());
					    	}
					    	
					    	try{
					    		memorydb1.connect();
					    	}catch(Exception e){
					    		logger.error("connect内存数据库"+memorydbhostipm+"异常"+e.getMessage());
					    	}
					    	
							if(memorydb1.isConnected()==false)
								j++;
							else
								i++;
							
							if(k>CONNECT_SECONDS)//测试10次内存数据库的链接
								break;
							try {
								Thread.sleep(1000);//测试间隔为1秒
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}
						if(j>i){
							//反复确认memorydb1为不可用，上一周期memorydb1为Master
							//因此，这周期需要调整Master，将memorydb2设为Master
							try{
								memorydb2.slaveofNoOne();
								try{
									master=memorydbhostips;
									S2=true;
									memorydb2.set("Master", memorydbhostips);
									logger.info("将"+memorydbhostips+"设为Master OK！");
									logger.info(memorydbhostipm+"不可用，需要等其可用时，才能将之设置为slave");
								}catch(Exception e){
									logger.warn("从"+memorydbhostips+"设置Master异常"+e.getClass());
								}
							}catch(Exception e){
								logger.warn("将"+memorydbhostips+"设为Master异常"+e.getClass());
							}
	
							//因memorydb1不可用，所以需要等待memorydb1可用是才能将它设为slave
							//设置本周期memorydb1和memorydb2的状态。
							ms1=false;
							ms2=true;
						}else{
							//反复确认memorydb1为可用,因为上一周期就是memorydb1为Master						
							//因此不用做Master设置变更，也不用重设内存数据库中的"Master"设置
							logger.info("Master "+memorydbhostipm+" is OK");
	
							//memorydb2可用，上一个周期就是memorydb2为slave
							//因此不用做Master设置变更，也不用重设内存数据库中的"Master"设置
							logger.info("Slave "+memorydbhostips+" is OK");
	
							//设置本周期memorydb1和memorydb2的状态。
							ms1=true;
							ms2=true;
						}
					}
					if(master.indexOf(memorydbhostips)>=0){
						logger.debug("32");
						//memorydb1连不上，上一周期memorydb2是Master					
						if(ms2==false){
							//如果上一周期memorydb2都连不是上
							//这一周期memorydb2才连上（有可能是这个周期是重启了才连上）
							//则需要将memorydb2强制设为master
							try{
								memorydb2.slaveofNoOne();
								try{
									master=memorydbhostips;
									S2=true;
									memorydb2.set("Master", memorydbhostips);
									logger.info("将"+memorydbhostips+"设为Master OK！");
									logger.info(memorydbhostipm+"不可用，需要等其可用时，才能将之设置为slave");
								}catch(Exception e1){
									logger.warn(memorydbhostips+"设置内存数据库Master"+e1.getMessage());
								}
							}catch(Exception e){
								logger.warn("将"+memorydbhostips+"设为Master 异常！"+e.getClass());
							}
						}
						S2=true;
						//设置本周期memorydb1和memorydb2的状态。
						ms1=false;
						ms2=true;
					}
				}else{//memorydb2连不上
					logger.debug("4");
					//memorydb1和memorydb2都连不上
					ms1=false;
					ms2=false;
					S2=true;
					logger.error(memorydbhostipm+"和"+memorydbhostips+"都连不上，请尽快核查！");
				}
			}//if1	
		}
	}//run	
	
	public void run0() {  
		logger.info("进入redis内存数据库定时监控线程monite");
		
		//public Jedis memorydb1=new Jedis(memorydbhostipm,memorydbportm);
		//public Jedis memorydb2=new Jedis(memorydbhostips,memorydbports);
		logger.info("链接memorydb1...");
		try{
			memorydb1.connect();//配置文件中的master
			logger.info("链接memorydb1成功");
		}catch(Exception e){
			logger.warn("链接内存数据库 "+memorydbhostipm+"出错："+e.getMessage());
		}
		
		logger.info("链接memorydb2...");
		try{
			memorydb2.connect();//配置文件中的slave
			logger.info("链接memorydb2成功");
		}catch(Exception e){
			logger.warn("链接内存数据库"+memorydbhostips+"出错："+e.getMessage());
		}
		
		logger.info("记录Master是"+master);

		if(memorydb1.isConnected()==true){
			if(master.indexOf(memorydbhostipm)>=0){
				//memorydb1可以连上，上周期Master为memorydb1
				if(ms1==false){
					//上一周期memorydb1没有连上，则视为本周期连上是重启了服务，因此要重新设定Master
					try{
						memorydb1.slaveofNoOne();
						memorydb1.set("Master", memorydbhostipm);
						master=memorydbhostipm;
						S2=true;//只要重新设定了Master，就应该重新设定Slave
						logger.info("本周期切换"+memorydbhostipm+"为 Master正常");
					}catch(Exception e){
						logger.warn("将"+memorydbhostipm+"设为master异常"+e.getMessage());
					}
				}else{
					//上一周期memorydb1连上了，且上周期Master为memorydb1，因此不需要重新设定Master
					logger.info(memorydbhostipm+"本周期正常，不需要进行master切换");
				}
				if(S2==true){
					//需要设定Slave为memorydbhostips
					if(memorydb2.isConnected()==true){
						try{
							memorydb2.slaveof(memorydbhostipm, memorydbportm);
							S2=false;
							logger.info("本周期切换 "+memorydbhostips+" 为 Slave 正常！");
						}catch(Exception e){
							logger.warn("将"+memorydbhostips+"设为"+memorydbhostipm+"的slave异常"+e.getMessage());
						}	
						//设置本周期memorydb2的状态。
						ms2=true;
					}else{
						//设置本周期memorydb2的状态。
						ms2=false;
						logger.warn("本周期需要切换 "+memorydbhostips+" 为Slave，但是该节点连不上，不能进行切换！");
					}
				}else{
					//设置本周期memorydb2的状态
					if(memorydb1.isConnected()==true)
						ms2=true;
					else
						ms1=false;
					logger.info("本周期不需要进行slave切换");
				}
			}
			if(master.indexOf(memorydbhostips)>=0){
				logger.info("上周期Master为memorydb2...");
				if(memorydb2.isConnected()==true){
					//memorydb2可以连接，上周期Master为memorydbhostips
					if(ms2==false){
						//上周期memorydb2没连上，是为本周期服务重启了，故要重新设定该节点为Master
						try{
							memorydb2.slaveofNoOne();
							memorydb2.set("Master", memorydbhostips);
							master=memorydbhostips;
							S2=true;
							logger.info("本周期切换 "+memorydbhostips+" 为 Master 正常！");
						}catch(Exception e){
							logger.warn("将"+memorydbhostips+"设为Master异常");
						}
					}
					if(S2==true){
						//需要设定Slave为memorydbhostipm
						try{
							memorydb1.slaveof(memorydbhostips, memorydbports);
							S2=false;
							logger.info("本周期切换 "+memorydbhostipm+" 为 Slave 正常！");
						}catch(Exception e){
							logger.warn("将"+memorydbhostipm+"设为"+memorydbhostips+"的slave异常"+e.getMessage());
						}
					}
					//设置本周期memorydb1和memorydb2状态
					ms2=true;
					ms1=true;
				}else{
					//memorydb1可以连上，memorydb2连不上
					//内存中memorydb2为master,故要反复确认memorydb2是否可用
					int i=0,j=0,k=0;
					while(true){
						k++;
				    	try{
					    	try { 
					    		memorydb2.quit(); 
					    		} catch (Exception e) { 
					    			logger.debug(""+memorydbhostips+"内存数据库quit异常："+e.getMessage());
					       		} 
				    		memorydb2.disconnect(); 
				    		} catch (Exception e) { 
				    			logger.debug(""+memorydbhostips+"内存数据库disconnect异常："+e.getMessage());
				    		}
				    	try{
				    		memorydb2.connect();
				    	}catch(Exception e){
				    		logger.error("connect内存数据库"+memorydbhostips+"异常"+e.getMessage());
				    	}
						if(memorydb2.isConnected()==false)
							j++;
						else
							i++;
						if(k>CONNECT_SECONDS)//测试2分钟内存数据库的链接
							break;
						try {
							Thread.sleep(1000);//测试间隔为1秒
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					if(j>i){
						//反复确认memorydb2不可用，memorydb1可用，
						//则将memorydb1设为Master,并将S2置为true
						try{
							memorydb1.slaveofNoOne();
							memorydb1.set("Master", memorydbhostipm);
							master=memorydbhostipm;
							S2=true;
							logger.info("将"+memorydbhostipm+"设为Master OK！");
						}catch(Exception e){
							logger.warn("将"+memorydbhostipm+"设为Master异常："+e.getMessage());
						}
						//设置本周期memorydb1和memorydb2的状态。
						ms1=true;
						ms2=false;
					}else{
						//反复确认Memorydb2可用，将memorydb2设为master
						try{
							memorydb2.slaveofNoOne();
							memorydb2.set("Master", memorydbhostips);
							master=memorydbhostips;
							S2=true;
							logger.info("将"+memorydbhostips+"设为Master OK！");
						}catch(Exception e){
							logger.warn("将"+memorydbhostips+"设为Master异常："+e.getMessage());
						}
						//memorydb1可用，所以同时设为slave
						try{
							memorydb1.slaveof(memorydbhostips, memorydbports);
							S2=false;
							logger.info("将"+memorydbhostips+"设为Master OK!");
						}catch(Exception e){
							logger.warn("将"+memorydbhostipm+"设为"+memorydbhostips+"的slave异常"+e.getMessage());
						}
						//设置本周期memorydb1和memorydb2的状态。
						ms1=true;
						ms2=true;
					}
				}
			}
			//设置本周期memorydb1的状态。
			ms1=true;
		}else{//memorydb1连不上
			if(memorydb2.isConnected()==true){
				if(master.indexOf(memorydbhostipm)>=0){
					//Master为memorydb1，但memorydb1连不上
					//反复确认memorydb1是否可用
					int i=0,j=0,k=0;
					while(true){
						k++;
				    	try{
					    	try { 
					    		memorydb1.quit(); 
					    		} catch (Exception e) { 
					    			logger.debug(memorydbhostipm+"内存数据库quit异常："+e.getMessage());
					       		} 
				    		memorydb1.disconnect(); 
				    		} catch (Exception e) { 
				    			logger.debug(memorydbhostipm+"内存数据库disconnect异常："+e.getMessage());
				    		}
				    	try{
				    		memorydb1.connect();
				    	}catch(Exception e){
				    		logger.error("connect内存数据库"+memorydbhostipm+"异常"+e.getMessage());
				    	}
						if(memorydb1.isConnected()==false)
							j++;
						else
							i++;
						if(k>CONNECT_SECONDS)//测试10秒内存数据库的链接
							break;
						try {
							Thread.sleep(1000);//测试间隔为1秒
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					if(j>i){
						//反复确认memorydb1为不可用，上一周期memorydb1为Master
						//因此，这周期需要调整Master，将memorydb2设为Master
						try{
							memorydb2.slaveofNoOne();
							memorydb2.set("Master", memorydbhostips);
							master=memorydbhostips;
							S2=true;
							logger.info("将"+memorydbhostips+"设为Master OK！");
							logger.info(memorydbhostipm+"不可用，需要等其可用时，才能将之设置为slave");
						}catch(Exception e){
							logger.warn("将"+memorydbhostips+"设为Master异常"+e.getClass());
						}
						//因memorydb1不可用，所以需要等待memorydb1可用是才能将它设为slave
						//设置本周期memorydb1和memorydb2的状态。
						ms1=false;
						ms2=true;
					}else{
						//反复确认memorydb1为可用,因为上一周期就是memorydb1为Master						
						//因此不用做Master设置变更，也不用重设内存数据库中的"Master"设置
						logger.info("Master "+memorydbhostipm+" is OK");

						//memorydb2可用，上一个周期就是memorydb2为slave
						//因此不用做Master设置变更，也不用重设内存数据库中的"Master"设置
						logger.info("Slave "+memorydbhostips+" is OK");

						//设置本周期memorydb1和memorydb2的状态。
						ms1=true;
						ms2=true;
					}
				}
				if(master.indexOf(memorydbhostips)>=0){
					//memorydb1连不上，上一周期memorydb2是Master					
					if(ms2==false){
						//如果上一周期memorydb2都连不是上
						//这一周期memorydb2才连上（有可能是这个周期是重启了才连上）
						//则需要将memorydb2强制设为master
						try{
							memorydb2.slaveofNoOne();
							memorydb2.set("Master", memorydbhostips);
							master=memorydbhostips;
							S2=true;
							logger.info("将"+memorydbhostips+"设为Master OK！");
							logger.info(memorydbhostipm+"不可用，需要等其可用时，才能将之设置为slave");
						}catch(Exception e){
							logger.warn("将"+memorydbhostips+"设为Master 异常！"+e.getClass());
						}
					}
					//设置本周期memorydb1和memorydb2的状态。
					ms1=false;
					ms2=true;
				}
			}else{//memorydb2连不上
				//memorydb1和memorydb2都连不上
				ms1=false;
				ms2=false;
				S2=true;
				logger.error(memorydbhostipm+"和"+memorydbhostips+"都连不上，请尽快核查！");
			}
		}//if1	
		//清除内存数据库连接
    	try{
	    	memorydb1.quit();
	    }catch(Exception e){
    		logger.warn("MemoryDB对象"+memorydbhostipm+"结束时，清除memorydbm出现异常!"+e.getMessage());
		}	
    	memorydb1.disconnect();
	    memorydb1=null;
	    
	    try{
	    	memorydb2.quit();
    	}catch(Exception e){
    		logger.warn("MemoryDB对象"+memorydbhostips+"结束时，清除memorydbs出现异常!"+e.getMessage());
		}	
	    memorydb2.disconnect();
	    memorydb2=null;
		logger.info("内存数据库定时监控管理线程运行结束");
	}//run	
}//class