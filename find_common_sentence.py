#usr/bin/python
# -*- coding: utf-8 -*-
import os
import re
import sys
from DataAccess import *
#from dedup_cluster import *
from httpclient import *
from getall_sentences import *
import Logger
import traceback
import UbClient
import mcpack

import socket
import struct

obj_topic_id = re.compile(r'^topic_id')
obj_best_title = re.compile(r'^best_title:')
obj_page_id = re.compile(r'^id:')
obj_delimi = re.compile(r'\t')
topic_list = []
logger = Logger.initlog('write_url.log')

dbhost = "127.0.0.1" #"10.10.208.13"
dbport = 1883 #int(sys.argv[2])

def writefile(destfileName,data2w):                                                                                                                          
    #wordF= file( destfileName,'w')      
    print '111dest---',destfileName
    #wordF= open( destfileName,'w+')      
    wordF= file( destfileName,'w')      
    print 'dest---',destfileName
    wordF.write('%s' %(data2w))     
    wordF.close()

def appendfile(destfileName,data2w):
    wordF= file( destfileName,'a')      
    wordF.write('%s' %(data2w))     
    wordF.close()

def writefile4int(destfileName,iValue):
    wordF= file( destfileName,'w')      
    wordF.write('%d' %(iValue))     
    wordF.close()

def readfile(destfileName):
    if not os.path.exists(destfileName):
        return ""
    singleFile= file( destfileName,'r') 
    singleFile.seek(0)     
    fContent = singleFile.read()
    singleFile.close()
    return fContent



def getdata(aids):
    logger = Logger.initlog('aa.log')
    total_num = 0
    category_num = 0 
    category_id = 0 

    if(1):
        results = DataAccess.get_url_by_aid(aids,logger)
        if results:
            category_num = 8;#//category_id # category_num + 1
            try:
                file_name = "urls_data_cluster/merged_cluster_url8"
                #+ str(category_num)
                file_url = open(file_name,'w+')

                for result in results:
                    publishtime = result[4]
                    fromsite= result[5]
                    fromsite= fromsite.encode('utf-8','ignore')
                    title = result[2].strip()
                    #print 'default-utf[', title ,"]\n"#= result[2].strip()
                    title = title.replace('\t',' ')
                    title = title.replace('\n',' ')
                    title = title.encode('utf-8','ignore')
                    #title = title.encode('gbk','ignore')
                    #print 'original--[',title ,"]\n"#= title.encode('gbk','ignore')
                    #print  title.encode('utf-8','ignore')
                    #print  title.decode("utf-8").encode("gbk")
                    #sleep(3) #summary = result[4].strip()
                    summary = result[3].strip()
                    summary = summary.replace('\t',' ')
                    summary = summary.replace('\n',' ')
                    #summary = summary.encode('gbk','ignore')
                    summary = summary.encode('utf-8','ignore')
                        #l2=len(summary)
                        #if(l1!=l2):
                        #    print 'replaced',l1,l2
                        #    raw_input()

                    url = result[1].strip()
                    if(url.find("dahe.cn")>0):
                        continue
                    url= url.replace('\t',' ')
                    url= url.replace('\n',' ')
                    #url = url.encode('gbk','ignore')
                    url = url.encode('utf-8','ignore')
                    aid = result[0]
                    content=summary
    #                if (len(summary)>1024):
    #                    try:
    #                        summary=summary[0:1024]
    #                    except:
    #                        print "except",aid
    #                    finally:
    #                        pass
                    if(title and url and publishtime):
                        file_url.write("%s\t" % (aid))
                        file_url.write("%s\t%s\t" %(title,summary))
                        file_url.write("%s\t%s\t%s\n" %(url,publishtime,content))
                        total_num = total_num + 1

                    else:
                        logger.info("tile or url or publishtime is null, ignored!")
            finally:
                file_url.close()
    #if res :
        #for category_id in res:
        
    #    else:
    #        logger.info("category: %s get urls null!" %(category_id[0]))
    #else:
    #    logger.info("get category list null!")
    #logger.info("total get urls :%d, category num :%d " %(total_num, category_num)) 


def getparser(title):#,content,aid,pubtime):
    pok=0
    termstr=""
    tryTimes=0;
    __terms=""
    while(tryTimes < 3):
        tryTimes=tryTimes+1
        #try:
        if(1):
            con=None
            if(con==None):
                dbtype = "socket"
                dbfile = ""
                dbwto = 1000
                dbrto = 1000
                logger = Logger.initlog("./con_sock_log")

                con = UbClient.Connector(dbtype, dbhost, dbport, dbfile ,dbwto, dbrto, logger) 
                con.sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
        
            reqdata = {}

            reqdata["title"]=title  
            reqdata["content"]=title
            reqdata["aid"]=0
            reqdata["pubtime"]=0

            req_pack = mcpack.dumps_version(mcpack.mcpackv1, reqdata, 4*1024*1024)
            con.write_pack(req_pack)
            (ret ,res_pack) = con.read_pack()
            pok=1
            if ret == 0:
                resdict = mcpack.loads(res_pack)
                
                __terms=resdict["terms"].replace("'"," ") 
                __terms = __terms.decode("gbk").encode('utf-8','ignore')  
                __termCount=resdict["termCount"]

                #print title
                #print __terms

    #                if(__terms!=""):
    #                    #print '-sock-ok',aid,__dupaids
    #                    dup_old_id=0
    #                    arr = __terms.split(';')
    #                    termcount=len(arr)
    #
    #                    reali=0;
    #                    _iterator_lock.acquire()  
    #                    _iterator_count+=1
    #                    reali=_iterator_count
    #                    lock.release() 
    #
    #                    if(reali % 10001 ==0):
    #                        print reali
    #
    #
    #                    if((termcount-1) !=int(__termCount)):
    #                        print "count not match",aid,termcount,__termCount
    #
    #                    termstr=__terms
                break;

        #except:
        if(0):
            print 'except-sock-',sys.exc_info()[0]
            con.sock.close()
            sleep(1)#con.sock.close()
            con=None

        if(con!=None):
            con.sock.close()
            con=None

    return __terms,__termCount

class ANode:
    def __init__(self,_sent,_mapdict):
        self.sent=_sent
        self.mapdict=_mapdict

    
def get_abstract(str_aids,keys):
    allstr = "";
    if(str_aids==None or len(str_aids)==0):
        return allstr

    str_aids=str_aids.strip(',')
    results = DataAccess.get_url_by_aid(str_aids,None)

    keyarr=keys.split('|')
    allsents=[]
    if results and len(results)>0:
        #try:
        if(1):

            for result in results:
                content = result[3]
                content=content.replace("\n"," ")
                content = content.encode("utf-8")
                content=content.replace("！","。").replace("？","。")
                sents=content.split("。")[0:5]
                _dict={}
                for s in sents:
                    for k1 in keyarr:
                        k=k1.strip('\t').strip()
                        if(s.find(k)>=0):
                            if(_dict.get(k)==None):
                                _dict[k]=1

                    _anode=ANode(s,_dict)
                    allsents.append(_anode)

            #topsents = sorted(allsents, lambda x, y: cmp(len(x[1]), len(y[1])), reverse=True)[0:20]
            topsents = sorted(allsents, lambda x, y: cmp(len(x.mapdict), len(y.mapdict)), reverse=True)[0:20]
            count=0;
            addedcount=0;
            addedsents=[]
            for s in topsents:

                if(len(s.sent)>256):
                    continue

                if(count==0):
                    allstr+=s.sent+"。| "
                    addedsents.append(s);
                    addedcount+=1;
                else:
                    issame=0;
                    matchedkeycount=0
                    not_matchedkeycount=0
                    for s1 in addedsents:
                        for k in s.mapdict:
                            if(s1.mapdict.get(k)):
                                matchedkeycount+=1
                                if(matchedkeycount>2):
                                    pass
                                    #issame=1
                                    #break;
                            else:
                                not_matchedkeycount+=1
                        if(matchedkeycount*100>((len(s.mapdict)+len(s1.mapdict))*50)):#not_matchedkeycount):
                            issame=1
                            break;
                    if(issame==0):
                        #print 'added more'
                        allstr+=s.sent+"。| "
                        #print allstr
                        addedsents.append(s);
                        addedcount+=1;
                        if(addedcount>2):
                            break;
                    else:
                        #print "no more-------"
                        pass

                count+=1

                
        #except:
            #print 'getsents error--'

    return allstr

def getstocks(str_aids):
    allstr = "";
    if(str_aids==None or len(str_aids)==0):
        return allstr

    str_aids=str_aids.strip(',')
    results = DataAccess.get_stocks_by_aid(str_aids)

    if results and len(results)>0:
            try:

                for result in results:
                    stockname = result[0]
                    allstr+=stockname +" | "
            except:
                print 'getstock error--'

    return allstr


def striphtml(data):
    p = re.compile(r'<.*?>')
    data = p.sub('', data)

    return data

delimiters=["！", "？" ,"；","。","；" ,"，"," "]

couple_delimiters=[["[","]"],["(",")"],["（","）"]]

#def getallsents(content):
#    sentlist=[]
#    contentlen=len(content)
#    cindex=0;
#    lastindex=0
#    while(cindex<contentlen):
#        if(cindex%101==0):
#            pass
#            ##print cindex
#
#        #o_index=cindex
#        found=0
#        for cple_d in couple_delimiters:
#            left_d=cple_d[0]
#            len_d=len(left_d)
#            if((cindex+len_d)<=contentlen):
#                cur_d=content[cindex:cindex+len_d]
#                if(cur_d==left_d):
#                    right_d=cple_d[1]
#                    right_len_d=len(right_d)
#                    max_len=39
#                    offset=1
#                    while(offset<max_len):
#                        cur_right_d=content[cindex+offset:cindex+offset+right_len_d]
#                        if(right_d==cur_right_d):
#                            #print offset,cur_d
#                            #cindex+=lend
#                            sent=content[lastindex:cindex]
#                            print sent
#                            sentlist.append(sent)
#
#                            sent2=content[cindex:cindex+offset+right_len_d]
#                            print sent2
#                            sentlist.append(sent2)
#
#                            lastindex=cindex+offset+right_len_d
#                            cindex+=offset
#
#                            found=1
#                            break
#
#                        offset+=1
#
#                    if(found==1):
#                        break
#
#                    #not matched with this couple_delimiter,then for next
#                    continue
#
#
#        #=[["[","]"],["(",")"],["（","）"]]
#
#        if(found==0):
#            for d in delimiters:
#                lend=len(d)
#                if((cindex+lend)<=contentlen):
#                    cur_d=content[cindex:cindex+lend]
#                    #print "cur_d",cindex,"[",cur_d,"][",d
#                    if(cur_d==d):
#                        if(1):#cindex>lastindex):
#                            cindex+=lend
#                            sent=content[lastindex:cindex]
#                            #print "sent",sent#=content[lastindex:cindex]
#                            #raw_input()
#
#                            #if(len(sent)>2):
#                            sentlist.append(sent)
#
#                            lastindex=cindex
#                            found=1
#                            break
#
#        if(found==0):
#            cindex+=1
#
#    #print sentlist
#    return sentlist
#                            
def readdata(file_topic,fname):
    allstr = "";
    item_num = 1
    flag = 0
    print "fname:",fname
    try:
        #lines = file_topic.readlines()
        #file_topic.close()
        filename = "./urls_data_cluster/result_url0"
        filter_dict={}#=process_filter(filename ,0)
        if(filter_dict==None):
            filter_dict={}

        aids=[]
        fromsite_sent_dict={}

        lastaid=0 #
        icount=0 #
        #while(1):
        if(1):
            if(icount%10001==0):
                print "linecount:",icount
                #break;
            results = DataAccess.get_url_for_common_sent()
            if results==None or  len(results)==0:
                pass
                #break;

            if results and len(results)>0:
                print lastaid,len(results)
                if(len(results)<10):
                    #break;
                    pass
                if(1):

                    for result in results:
                        publishtime = result[4]

                        aid = result[0]

                        if(lastaid<aid): 
                            lastaid=aid 

                        #try:
                        if(1):
                            url = result[1].strip () 
                            url = url.replace ('\t', ' ') 
                            url = url.replace ('\n', ' ') 
                            url = url.encode ('utf-8', 'ignore') 

                            _host, _port, _dir, _path = get_domain_port_dir (url) 
                            fromsite="" # _host


                            if(result[3]==None):
                                print "content is none",url,aid
                                continue

                            summary = result[3].strip()
                            summary = striphtml(summary)
                            summary = summary.replace('\t',' ')
                            summary = summary.replace('\n',' ')
                            summary = summary.encode('utf-8','ignore')
                            content = summary


                            icount+=1
                            sent_dict={}
                            if(fromsite_sent_dict.get(fromsite)):
                                sent_dict=fromsite_sent_dict[fromsite]
                            else:
                                fromsite_sent_dict[fromsite]=sent_dict

                            sentlist=getallsents(content)
                            for sent in sentlist:
                                if(len(sent)>2):
                                    if(sent_dict.get(sent)):
                                        sent_dict[sent]+=1
                                    else:
                                        sent_dict[sent]=1

                            
        print "dict len:", len(fromsite_sent_dict.items())
        for k,v in fromsite_sent_dict.items():

            sent_dict=v
            topsents = sorted(sent_dict.items(), lambda x, y: cmp(x[1], y[1]),reverse=True)[0:100000]

            #allstr +="fromsite__:"+k+"\n"
            count=0;
            addedcount=0;
            addedsents=[]
            #allstr=""
            for s in topsents:
                if(s[1]<7):
                    break;

                #allstr +="\t"+ s[0]+ "\t" +str(s[1])+"\n"
                allstr += s[0]+ "\t" +str(s[1])+"\n"

        fname="./commen_sents" #fname.replace("result_merged_cluster_url","result_merged_cluster_url_topic")
        writefile(fname,allstr)
        straids=""
        return None#straids

    except:
        print "traceback", traceback.print_exc()
        logger.info("read file urls/* failed!")
    finally:
        pass



def process(filename):
    global topic_list
    detail = {}
    pages = [] #the pages in this topic
    allstr = "";
    item_num = 1
    flag = 0
    try:
        #fname= "./urls_data_cluster/url0"
        #fname= "./urls_data_cluster/url_dedeped"
        #result_merged_cluster_url24_0"
        file_topic = None#open(fname)
        #straids=readdata(None,"")#file_topic,fname)
        readdata(None,"")#file_topic,fname)

    except:
        print "traceback", traceback.print_exc()
        #print 'except--',sys.exc_info()[0]
        logger.info("read file urls/* failed!")
    finally:
        pass
if __name__ == "__main__":
    #dir_path = './urls_data_cluster/'
    #files  = os.listdir(dir_path)
    ctnt="啊。吧，才！"
    ctnt="123[责任编辑:邱晓琴]aa"##(Clara Bow) 、葛丽泰·嘉宝 ("#啊。吧，才"

    #getallsents(ctnt)
    #sentlist=getallsents(ctnt)
    #print sentlist
    #exit()

    process("")#dir_path + filename)

    #for filename in files:
    #    topic_list = []
        #if re.match(re.compile(r'^result_url'),filename):


    print 'over--'
