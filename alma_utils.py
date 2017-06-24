#alma_utils.py

from urllib.request import Request, urlopen
from urllib.parse import urlencode, quote_plus
import xml.etree.ElementTree as ET
from lxml import etree
import json
import codecs
import time
import pandas as pd
import io
import os 

class Alma:
    '''Alma is a set of tools for adding and manipulating Alma bib records'''

    def __init__(self):
        pass
        return

    def get_representation(mms_id):
        '''get_representation:
        Parameter: an mms_id
        returns a dictionary containing:
        the id for a digital representation attached to the bib record if the representation exists
        the mms_id
        the oai record identifer
        '''
        url = 'https://api-na.hosted.exlibrisgroup.com/almaws/v1/bibs/{mms_id}/representations'.replace('{mms_id}',quote_plus(mms_id))
        queryParams = '?' + urlencode({ quote_plus('apikey') : 'l7xxf2bd5e66223d4008b04eba8751a96856'  })
        request = Request(url + queryParams)
        request.get_method = lambda: 'GET'
        response_body = urlopen(request).read().decode()
        root = ET.fromstring(response_body)
        rep_xml = ''
        d = {}
        d['mms_id'] = mms_id
        d['originating_record_id'] = 'none'
        d['id'] = 'none'
        try:
            for kid in root[0].getchildren():
                d[kid.tag] = kid.text.replace('%3A',':').replace('%2F','/')
        except Exception as e:
            d['id'] = 'none'
            pass
        if '1161' in d['id']:
            #print('Found it: ',d['id'])

            url = 'https://api-na.hosted.exlibrisgroup.com/almaws/v1/bibs/{mms_id}/representations/{rep_id}'.replace('{mms_id}',quote_plus(mms_id)).replace('{rep_id}',quote_plus(d['id']))
            queryParams = '?' + urlencode({ quote_plus('apikey') : 'l7xxf2bd5e66223d4008b04eba8751a96856'  }) ## production
            #queryParams = '?' + urlencode({ quote_plus('apikey') : 'l7xx4f9174fae8bc49a780ba6d5a93151af0'  }) ## sandbox
            request = Request(url + queryParams)
            request.get_method = lambda: 'GET'
            rep_xml = urlopen(request).read().decode()
            #print('get_reps: ',d)
        return(d,rep_xml)  

    def add_ia_representation(mms_id,identifier,rights):
        '''add_representation adds a digital reprentation record to a bib record in Alma for a
        digital object residing in an institutional repository
        Parameters: mms_id, the OAI record identifier
        Returns the mms_id, the OAI record identifier, and the ID for the digital representation'''
        if rights == 'pd':
            rights = 'Public Domain : You can copy, modify, distribute and perform the work, even for commercial purposes, all without asking permission.'
        if rights == 'pdus':
            rights = "Public Domain (US) : You can copy, modify, distribute and perform the work, even for commercial purposes, all without asking permission in the U.S."
        if rights == 'cc-by-nc-nd-3.0':
            rights += 'This work is protected by copyright law, but made available under a Creative Commons Attribution-NonCommercial-NoDerivatives license.'
        if rights == 'cc-by-nc-nd-4.0':
            rights += 'This work is protected by copyright law, but made available under a Creative Commons Attribution-NonCommercial-NoDerivatives license.'
        if rights == 'cc-by-nc-3.0':
            rights += 'This work is protected by copyright law, but made available under a Creative Commons Attribution-NonCommercial license.'
        if rights == 'cc-by-nc-4.0':
            rights += 'This work is protected by copyright law, but made available under a Creative Commons Attribution-NonCommercial-ShareAlike license.'
        if rights == 'cc-by-nc-sa-4.0':
            rights += 'This work is protected by copyright law, but made available under a Creative Commons Attribution-NonCommercial-ShareAlike license.'
        if rights == 'cc-by-nc-sa-3.0':
            rights += 'This work is protected by copyright law, but made available under a Creative Commons Attribution-NonCommercial-ShareAlike license.'
        if rights == 'cc-by-3.0':
            rights += 'This work is protected by copyright law, but made available under a Creative Commons Attribution license.'
        if rights == 'cc-by-4.0':
            rights += 'This work is protected by copyright law, but made available under a Creative Commons Attribution license.'

        delivery_url = identifier.replace('%3A',':').replace('%2F','/') 
        linking_parameter = identifier 
        url = 'https://api-na.hosted.exlibrisgroup.com/almaws/v1/bibs/{mms_id}/representations'.replace('{mms_id}',quote_plus(mms_id))
        #queryParams = '?' + urlencode({ quote_plus('apikey') : 'l7xx4f9174fae8bc49a780ba6d5a93151af0'  }) ## sandbox
        queryParams = '?' + urlencode({ quote_plus('apikey') : 'l7xxf2bd5e66223d4008b04eba8751a96856'  }) ## production
        values  = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
                 <representation is_remote="true">    
                 <id />    
                 <library desc="Mugar">MUG</library>    
                 <usage_type desc="Derivative">DERIVATIVE_COPY</usage_type> 
                 <public_note>{rights}</public_note>
                 <delivery_url>{delivery_url}</delivery_url>    
                 <thumbnail_url/>    
                 <repository desc="InternetArchive">InternetArchive</repository>  
                 <originating_record_id>{identifier}</originating_record_id>    
                 <linking_parameter_1>{linking_parameter}</linking_parameter_1>    
                 <linking_parameter_2/>    
                 <linking_parameter_3/>    
                 <linking_parameter_4/>    
                 <linking_parameter_5/>    
                 <created_by>jwasys</created_by>    
                 <created_date>2017-04-28Z</created_date>    
                 <last_modified_by>jwasys</last_modified_by>    
                 <last_modified_date>2017-04-28Z</last_modified_date>
                 </representation>'''
        values = values.replace('{mms_id}',quote_plus(mms_id)).replace('{identifier}',quote_plus(linking_parameter))
        values = values.replace('{rights}',rights).replace('\n','')
        values = values.replace('{linking_parameter}',quote_plus(linking_parameter)).replace('{delivery_url}',quote_plus(delivery_url)).encode("utf-8")
        headers = {  'Content-Type':'application/xml'  }
        request = Request(url + queryParams
        , data=values
        , headers=headers)
        request.get_method = lambda: 'POST'
        response_body = urlopen(request).read()
        tree = etree.fromstring(response_body)
        x = tree.find('id')
        return(mms_id,identifier,x.text)


    def add_ht_representation(mms_id,identifier,rights):
        '''add_representation adds a digital reprentation record to a bib record in Alma for a
        digital object residing in an institutional repository
        Parameters: mms_id, the OAI record identifier
        Returns the mms_id, the OAI record identifier, and the ID for the digital representation'''
        if rights == 'pd':
            rights = 'Public Domain : You can copy, modify, distribute and perform the work, even for commercial purposes, all without asking permission.'
        if rights == 'pdus':
            rights = "Public Domain (US) : You can copy, modify, distribute and perform the work, even for commercial purposes, all without asking permission in the U.S."
        if rights == 'cc-by-nc-nd-3.0':
            rights += 'This work is protected by copyright law, but made available under a Creative Commons Attribution-NonCommercial-NoDerivatives license.'
        if rights == 'cc-by-nc-nd-4.0':
            rights += 'This work is protected by copyright law, but made available under a Creative Commons Attribution-NonCommercial-NoDerivatives license.'
        if rights == 'cc-by-nc-3.0':
            rights += 'This work is protected by copyright law, but made available under a Creative Commons Attribution-NonCommercial license.'
        if rights == 'cc-by-nc-4.0':
            rights += 'This work is protected by copyright law, but made available under a Creative Commons Attribution-NonCommercial-ShareAlike license.'
        if rights == 'cc-by-nc-sa-4.0':
            rights += 'This work is protected by copyright law, but made available under a Creative Commons Attribution-NonCommercial-ShareAlike license.'
        if rights == 'cc-by-nc-sa-3.0':
            rights += 'This work is protected by copyright law, but made available under a Creative Commons Attribution-NonCommercial-ShareAlike license.'
        if rights == 'cc-by-3.0':
            rights += 'This work is protected by copyright law, but made available under a Creative Commons Attribution license.'
        if rights == 'cc-by-4.0':
            rights += 'This work is protected by copyright law, but made available under a Creative Commons Attribution license.'

        delivery_url = identifier.replace('%3A',':').replace('%2F','/') 
        linking_parameter = identifier 
        url = 'https://api-na.hosted.exlibrisgroup.com/almaws/v1/bibs/{mms_id}/representations'.replace('{mms_id}',quote_plus(mms_id))
        #queryParams = '?' + urlencode({ quote_plus('apikey') : 'l7xx4f9174fae8bc49a780ba6d5a93151af0'  }) ## sandbox
        queryParams = '?' + urlencode({ quote_plus('apikey') : 'l7xxf2bd5e66223d4008b04eba8751a96856'  }) ## production
        values  = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
                 <representation is_remote="true">    
                 <id />    
                 <library desc="Mugar">MUG</library>    
                 <usage_type desc="Derivative">DERIVATIVE_COPY</usage_type> 
                 <public_note>{rights}</public_note>
                 <delivery_url>{delivery_url}</delivery_url>    
                 <thumbnail_url/>    
                 <repository desc="HathiTrust">HathiTrust</repository>  
                 <originating_record_id>{identifier}</originating_record_id>    
                 <linking_parameter_1>{linking_parameter}</linking_parameter_1>    
                 <linking_parameter_2/>    
                 <linking_parameter_3/>    
                 <linking_parameter_4/>    
                 <linking_parameter_5/>    
                 <created_by>jwasys</created_by>    
                 <created_date>2017-04-28Z</created_date>    
                 <last_modified_by>jwasys</last_modified_by>    
                 <last_modified_date>2017-04-28Z</last_modified_date>
                 </representation>'''
        values = values.replace('{mms_id}',quote_plus(mms_id)).replace('{identifier}',quote_plus(linking_parameter))
        values = values.replace('{rights}',rights).replace('\n','')
        values = values.replace('{linking_parameter}',quote_plus(linking_parameter)).replace('{delivery_url}',quote_plus(delivery_url)).encode("utf-8")
        headers = {  'Content-Type':'application/xml'  }
        request = Request(url + queryParams
        , data=values
        , headers=headers)
        request.get_method = lambda: 'POST'
        #return(values)
        response_body = urlopen(request).read()
        tree = etree.fromstring(response_body)
        x = tree.find('id')
        return(mms_id,identifier,x.text)


    def get_bib_from_alma(mms_id):
            ## first get the current bib 
        url = 'https://api-na.hosted.exlibrisgroup.com/almaws/v1/bibs/{mms_id}'.replace('{mms_id}',quote_plus(mms_id))
        queryParams = '?' + urlencode({ quote_plus('expand') : 'None' ,quote_plus('apikey') : 'l7xxf2bd5e66223d4008b04eba8751a96856'  }) ## production
        #queryParams = '?' + urlencode({ quote_plus('expand') : 'None' ,quote_plus('apikey') : 'l7xx4f9174fae8bc49a780ba6d5a93151af0'  }) ## sandbox
        request = Request(url + queryParams)
        request.get_method = lambda: 'GET'
        response_body = urlopen(request).read().decode()
        return(response_body)

    def update_bib(mms_id,ht_recno,rights):

        ## first get the current bib 
        url = 'https://api-na.hosted.exlibrisgroup.com/almaws/v1/bibs/{mms_id}'.replace('{mms_id}',quote_plus(mms_id))
        queryParams = '?' + urlencode({ quote_plus('expand') : 'None' ,quote_plus('apikey') : 'l7xxf2bd5e66223d4008b04eba8751a96856'  }) ## production
        #queryParams = '?' + urlencode({ quote_plus('expand') : 'None' ,quote_plus('apikey') : 'l7xx4f9174fae8bc49a780ba6d5a93151af0'  }) ## sandbox
        request = Request(url + queryParams)
        request.get_method = lambda: 'GET'
        response_body = urlopen(request).read()
        #%pdb
        rights_dict = {'pd':'public domain','pdus':'public domain (US)','icworld':'in copyright (world)',
                   'icus':'in copyright (US)','ic':'in copyright','und':'unknown',
                   'cc-by-nc-nd-3.0': 'Creative Commons Attribution-NonCommercial-NoDerivatives',
                   'cc-by-nc-nd-4.0': 'Creative Commons Attribution-NonCommercial-NoDerivatives',
                   'cc-by-nc-3.0': 'Creative Commons Attribution-NonCommercial',
                   'cc-by-nc-4.0': 'Creative Commons Attribution-NonCommercial',
                   'cc-by-nc-sa-4.0': 'Creative Commons Attribution-NonCommercial-ShareAlike',
                   'cc-by-nc-sa-3.0': 'Creative Commons Attribution-NonCommercial-ShareAlike',
                   'cc-by-sa-4.0': 'Creative Commons Attribution-ShareAlike',
                   'cc-by-sa-3.0': 'Creative Commons Attribution-ShareAlike',
                   'cc-by-3.0': 'Creative Commons Attribution',
                   'cc-by-4.0': 'Creative Commons Attribution',
                   }

        ## now let's add the ht_recno to 024$a
        if rights in rights_dict:
            rights = rights_dict[rights]

        _024 = '''       <datafield ind1=" 7" ind2=" " tag="024">
                <subfield code="a">{{ht_record}}</subfield>
                <subfield code="c">{{rights}}</subfield>
                <subfield code="2">HathiTrust</subfield>
                <subfield code="0">http://catalog.hathitrust.org/Record/{{ht_record}}</subfield>
            </datafield>'''.replace('{{ht_record}}',ht_recno).replace('{{rights}}',rights)
        _924 = '''       <datafield ind1=" 7" ind2=" " tag="924">
                <subfield code="a">{{ht_record}}</subfield>
                <subfield code="c">{{rights}}</subfield>
                <subfield code="2">HathiTrust</subfield>
                <subfield code="0">http://catalog.hathitrust.org/Record/{{ht_record}}</subfield>
            </datafield>'''.replace('{{ht_record}}',ht_recno).replace('{{rights}}',rights)

        field_924 = etree.fromstring(_924)
        field = etree.fromstring(_024)
        bib = etree.fromstring(response_body)
        field024=bib.findall('./record/datafield/[@tag="024"]')
        #print(len(field024))
        f024 = False
        for i in field024:
            suba = i.find('*/[@code="a"]')
            sub2 = i.find('*/[@code="2"]')
            subc = i.find('*/[@code="c"]')
            #print(sub2.text)
            try:
                if sub2.text == 'HathiTrust':
                    f024 = True
                    subc.text = rights
            except Exception as e:
                pass
        if not f024:    
            record = bib.find('./record')
            record.append(field)
        field924=bib.findall('./record/datafield/[@tag="924"]')
        if len(field924) == 0:
            record = bib.find('./record')
            record.append(field_924)

        url = 'https://api-na.hosted.exlibrisgroup.com/almaws/v1/bibs/{mms_id}'.replace('{mms_id}',quote_plus(mms_id))
        queryParams = '?' + urlencode({ quote_plus('apikey') : 'l7xxf2bd5e66223d4008b04eba8751a96856'  }) ## production
        #queryParams = '?' + urlencode({ quote_plus('apikey') : 'l7xx4f9174fae8bc49a780ba6d5a93151af0'  }) ## sandbox
        values  = ET.tostring(bib)
        headers = {  'Content-Type':'application/xml'  }
        request = Request(url + queryParams, data=values, headers=headers)
        request.get_method = lambda: 'PUT'
        response_body = urlopen(request).read()
        return(response_body)

    def get_marc_fields(tag,bib):
        fields = bib.findall("*/[@tag='" + tag +"']")
        return(fields)

    def has_marc_field(tag,bib):
        field = record.find("*/[@tag='" + tag +"']")
        return(type(field) == ET.Element)
    
    @staticmethod
    def sort_marc_tags(record):
        data = []
        for elem in record.getchildren():
            if 'leader' in elem.tag: #== 'http://www.loc.gov/MARC21/slim}leader':
                data.append(('000', elem))
            else:
                attrib = elem.attrib
                for k,v in attrib.items():
                    if k == 'tag':
                        data.append((v,elem))
        data = sorted(data, key=lambda x: x[0])
        new_rec = ET.Element('record')
        for i in data:
            new_rec.append(i[1])
        return(new_rec)

    def alma_rec_update(mms_id,oai_ident):
        bib =get_bib_from_alma(mms_id)
        obu_rec = ET.tostring(get_openBU_results(oai_ident)[1])
        obu_rec = obu_rec.decode().replace('ns0:','').replace(' xmlns:ns0="http://www.loc.gov/MARC21/slim"','').replace('\n','')
        obu_rec = ET.fromstring(obu_rec)
        bib_record = ET.fromstring(bib)
        record = bib_record.find('./record')
        for datafield in record.findall('datafield'):
            #print(datafield)
            try:
                record.remove(datafield)
            except ValueError as e:
                pass
        for datafield in obu_rec.findall('datafield'):
            #print(datafield.tag,datafield.attrib,datafield.text)
            record.append(datafield)
        bib_record_leader = bib_record.find('*/leader')
        obu_rec_leader = obu_rec.find('leader')
        bib_record_leader.text = '     nam a22001691a 4500'

        #return(bib_record,obu_rec)
        url = 'https://api-na.hosted.exlibrisgroup.com/almaws/v1/bibs/{mms_id}'.replace('{mms_id}',quote_plus(mms_id))
        queryParams = '?' + urlencode({ quote_plus('apikey') : 'l7xxf2bd5e66223d4008b04eba8751a96856'  }) ## production
        #queryParams = '?' + urlencode({ quote_plus('apikey') : 'l7xx4f9174fae8bc49a780ba6d5a93151af0'  }) ## sandbox
        values  = ET.tostring(bib_record)
        headers = {  'Content-Type':'application/xml'  }
        request = Request(url + queryParams, data=values, headers=headers)
        request.get_method = lambda: 'PUT'
        response_body = urlopen(request).read()
        return(response_body)

    @classmethod
    def alma_rec_create(cls,oai_ident,rights,etd,r):
        recno = str(oai_ident)[16:]
        if type(rights) != str:
            rights = 'Undetermined'
            #rights = 'cc-by-nc-sa-4.0'
        rights_dict = {'pd':'public domain','pdus':'public domain (US)','icworld':'in copyright (world)',
                   'icus':'in copyright (US)','ic':'in copyright','und':'unknown',
                   'cc-by-nc-nd-3.0': 'Creative Commons Attribution-NonCommercial-NoDerivatives',
                   'cc-by-nc-nd-4.0': 'Creative Commons Attribution-NonCommercial-NoDerivatives',
                   'cc-by-nc-3.0': 'Creative Commons Attribution-NonCommercial',
                   'cc-by-nc-4.0': 'Creative Commons Attribution-NonCommercial',
                   'cc-by-nc-sa-4.0': 'Creative Commons Attribution-NonCommercial-ShareAlike',
                   'cc-by-nc-sa-3.0': 'Creative Commons Attribution-NonCommercial-ShareAlike',
                   'cc-by-sa-4.0': 'Creative Commons Attribution-ShareAlike',
                   'cc-by-sa-3.0': 'Creative Commons Attribution-ShareAlike',
                   'cc-by-3.0': 'Creative Commons Attribution',
                   'cc-by-4.0': 'Creative Commons Attribution',
                   }

        bib = ET.Element('bib')
        record = ET.Element('record')
        leader = ET.SubElement(record,'leader')
        leader.text = '     nam a22001691a 4500'
        field = {'tag':'007','ind1':' ','ind2':' ','text':'cr |||---||a|p'}
        f = cls.make_field(field,[]) ## create an 007
        record.append(f)
    #    field = {'tag':'008','ind1':' ','ind2':' ','text':'121103s1941####mau####fsm ###00000#eng#d'}
    #    f = cls.make_field(field,[]) ## create an 008
    #    record.append(f)
        _024 = '''       <datafield ind1="7" ind2=" " tag="024">
                <subfield code="a">{{OpenBU_record}}</subfield>
                <subfield code="c">{{rights}}</subfield>
                <subfield code="2">OpenBU</subfield>
                <subfield code="0">http://hdl.handle.net/{{record}}</subfield>
            </datafield>'''.replace('{{OpenBU_record}}',oai_ident).replace('{{rights}}',rights).replace('{{record}}',recno)
        _924 = '''       <datafield ind1="7" ind2=" " tag="924">
                <subfield code="a">{{OpenBU_record}}</subfield>
                <subfield code="c">{{rights}}</subfield>
                <subfield code="2">OpenBU</subfield>
                <subfield code="0">http://hdl.handle.net/{{record}}</subfield>
            </datafield>'''.replace('{{OpenBU_record}}',oai_ident).replace('{{rights}}',rights).replace('{{record}}',recno)

        field_924 = ET.fromstring(_924)
        field_024 = ET.fromstring(_024)
        record.append(field_024)
        record.append(field_924)

        field = {'tag':'336','ind1':' ','ind2':' '}
        subfields = [{'code':'a','text':'computer'},{'code':'b','text':'c'},
                     {'code':'2','text':'rdamedia'},{'code':'0','text':'http://id.loc.gov/vocabulary/mediaTypes/c'}]
        f = cls.make_field(field,subfields) ## create an 336
        record.append(f)
        field = {'tag':'337','ind1':' ','ind2':' '}
        subfields = [{'code':'a','text':'Academic theses'},{'code':'b','text':'gf2014026039'},
                     {'code':'2','text':'lcgft'},{'code':'0','text':'http://id.loc.gov/authorities/genreForms/gf2014026039'}]
        f = cls.make_field(field,subfields) ## create an 337
        record.append(f)
        field = {'tag':'338','ind1':' ','ind2':' '}
        subfields = [{'code':'a','text':'online resource'},{'code':'b','text':'cr'},
                     {'code':'2','text':'lcgft'},{'code':'0','text':'http://id.loc.gov/vocabulary/carriers/cr'}]
        f = cls.make_field(field,subfields) ## create an 338
        record.append(f) 
        ##
        ## work from here to accomodate no harvestable record
        ##
        try:
            obu_results = Dspace.get_openBU_results(oai_ident,rights)
            if obu_results[0] == 'deleted':
                print('record deleted : ',oai_ident)
                return('deleted')
            obu_rec = ET.tostring(obu_results[1])
            obu_rec = obu_rec.decode().replace('ns0:','').replace('xmlns:ns0="http://www.loc.gov/MARC21/slim"','').replace('\n','')
            obu_rec = ET.fromstring(obu_rec)
            for field in obu_rec.findall('*/[@tag]'):
                record.append(field)
        except Exception as e:
            print("Can't get OBU record. Creating from spreadsheet")

            ## creator
            field = {'tag':'100','ind1':'1','ind2':' '}
            author = r['dc.contributor.author']
            subfields = [{'code':'a','text':author[0:author.index('::')]}]
            f = cls.make_field(field,subfields) ## create an 100
            record.append(f)
            ## title
            field = {'tag':'245','ind1':' ','ind2':' '}
            title = r['dc.title[en_US]']
            subfields = [{'code':'a','text':title}]
            f = cls.make_field(field,subfields) ## create an 245
            record.append(f) 
            ## note
            field = {'tag':'500','ind1':' ','ind2':' '}
            note = r['dc.description[en_US]']
            subfields = [{'code':'a','text':note[note.index('\n')+2:]}]
            f = cls.make_field(field,subfields) ## create an 245
            record.append(f) 
            ## abstract
            field = {'tag':'520','ind1':' ','ind2':' '}
            abstract = r['dc.description.abstract[]']
            subfields = [{'code':'a','text':note}]
            f = cls.make_field(field,subfields) ## create an 245
            record.append(f) 



        _260 = record.find('*/[@tag="260"]')
        try:
            cdate = '©'+_260.find('*/[@code="c"]').text
            date = cdate[1:]
        except:
            date = r['dc.date.issued']
            cdate = '©'+date
        #print(date)
        text = '121103s{{date}}####mau####fsm ###00000#eng#d'
        text = text.replace('{{date}}',date)
        field = {'tag':'008','ind1':' ','ind2':' ','text':text}
        f = cls.make_field(field,[]) ## create an 008
        record.append(f)
        try:
            record.remove(_260)
        except Exception as e:
            pass
        field = {'tag':'264','ind1':'3','ind2':'4'}
        subfields = [{'code':'a','text':'Boston'},{'code':'b','text':'Boston University'},
                     {'code':'c','text':cdate}]
        f = cls.make_field(field,subfields)
        record.append(f)
        _264 = ET.Element('datafield')
        _264.attrib = {'tag':'264','ind1':'3','ind2':'4'}
        field = {'tag':'502','ind1':' ','ind2':' '}
        subfields = []
        if type(etd['name']) == str:
            subfields.append({'code':'a','text':'Thesis (' + etd['name']+ ')--Boston University,'+ date + '.'})
            subfields.append({'code':'b','text':etd['name']})
        subfields.append({'code':'c','text':'Boston University'})
        subfields.append({'code':'d','text':date})
        if type(etd['discipline']) == str:
            subfields.append({'code':'g','text':etd['discipline']})
        f = cls.make_field(field,subfields)
        record.append(f)

        ## Here we try to harvest some subject headings from Primo
        title = record.find('./*/[@tag="245"]/*/[@code="a"]').text + ' '
        author = record.find('./*/[@tag="100"]/*/[@code="a"]').text + ' proquest'
        topics = []
        try:
            json_str = json.loads(get_primo_results(build_url(title + author,'1')).decode())
            topics = json_str['SEGMENTS']['JAGROOT']['RESULT']['DOCSET']['DOC']['PrimoNMBib']['record']['display']['subject'].split(';')
        except Exception as e:
            pass
        for i in range(0,len(topics)):
            topics[i] = topics[i].lstrip().rstrip()

        for topic in set(topics):
            field = {'tag': '653', 'ind1':'0', 'ind2': '0'}
            subfields = [{'code':'a','text': topic}]
            f = cls.make_field(field,subfields)
            record.append(f)
        _024s = record.findall('./*/[@tag="024"]')
        if len(_024s) > 1:
            for _024 in _024s[1:]:
                record.remove(_024)
        _924s = record.findall('./*/[@tag="924"]')
        if len(_924s)>1:
            for _924 in _924s[1:]:
                record.remove(_924)

        bib.append(cls.sort_marc_tags(record))
        ## comment next line out to finish creating the alma record
        #return(bib)

        url = 'https://api-na.hosted.exlibrisgroup.com/almaws/v1/bibs'
        queryParams = '?' + urlencode({ quote_plus('apikey') : 'l7xxf2bd5e66223d4008b04eba8751a96856'  }) ## production
        #queryParams = '?' + urlencode({ quote_plus('apikey') : 'l7xx4f9174fae8bc49a780ba6d5a93151af0'  }) ## sandbox
        values  = ET.tostring(bib)
        #'<bib><record><leader>     nam a22001691a 4500</leader><controlfield ind1=" " ind2=" " tag="007">cr |||---||a|p</controlfield><controlfield ind1=" " ind2=" " tag="008">121103s1941####mau####fsm ###00000#eng#d</controlfield><datafield ind1=" 7" ind2=" " tag="024">            <subfield code="a">oai:open.bu.edu:2144/15687</subfield>            <subfield code="c">Undetermined</subfield>            <subfield code="2">OpenBU</subfield>            <subfield code="0">http://hdl.handle.net/2144/15687</subfield>        </datafield><datafield ind1=" " ind2=" " tag="042"><subfield code="a">dc</subfield></datafield><datafield ind1="1" ind2=" " tag="100"><subfield code="a">Schoenberger, Melissa</subfield><subfield code="e">author</subfield></datafield><datafield ind1="0" ind2="0" tag="245"><subfield code="a">Cultivating the arts of peace: English Georgic poetry from Marvell to Thomson</subfield></datafield><datafield ind1="3" ind2="4" tag="264"><subfield code="a">Boston</subfield><subfield code="b">Boston University</subfield><subfield code="c">&#169;2015</subfield></datafield><datafield ind1=" " ind2=" " tag="336"><subfield code="a">computer</subfield><subfield code="b">c</subfield><subfield code="2">rdamedia</subfield><subfield code="0">http://id.loc.gov/vocabulary/mediaTypes/c</subfield></datafield><datafield ind1=" " ind2=" " tag="337"><subfield code="a">Academic theses</subfield><subfield code="b">gf2014026039</subfield><subfield code="2">lcgft</subfield><subfield code="0">http://id.loc.gov/authorities/genreForms/gf2014026039</subfield></datafield><datafield ind1=" " ind2=" " tag="338"><subfield code="a">online resource</subfield><subfield code="b">cr</subfield><subfield code="2">lcgft</subfield><subfield code="0">http://id.loc.gov/vocabulary/carriers/cr</subfield></datafield><datafield ind1=" " ind2=" " tag="502"><subfield code="a">Thesis(Ph.D.)--Boston University,2015.</subfield><subfield code="b">Ph.D.</subfield><subfield code="c">Boston University</subfield><subfield code="d">2015</subfield><subfield code="g">English</subfield></datafield><datafield ind1=" " ind2=" " tag="520"><subfield code="a">Virgil\\'s Georgics portray peace and war as disparate states derived from the same fundamental materials. Adopting a didactic tone, the poet uses the language of farming to confront questions about the making of lasting peace in the wake of the Roman civil wars. Rife with subjunctive constructions, the Georgics place no hope in the easily realized peace of a golden age; instead, they teach us that peace must be sowed, tended, reaped, and replanted, year after year. Despite this profound engagement with the consequences of civil war, however, the Georgics have not often been studied in relation to English writers working after the civil wars of the 1640s. I propose that we can better understand poems by Andrew Marvell, John Dryden, Anne Finch, and John Philips--all of whom grappled with the ramifications of war--by reading their work in relation to the georgic peace of Virgil\\'s poem. In distinct ways, these poets question the dominant myth of a renewed golden age; instead, they model peace as a stable yet contingent condition constructed from chaotic materials, and therefore in need of perpetual maintenance. This project contributes to existing debates on genre, classical translation, the relationships between early modern poetry and politics, and most importantly, poetic representations of political and social peace. Recent work has argued for the georgic as a flexible mode rather than a formal genre, yet scholars remain primarily interested in its relation to questions of British national identity, agricultural reform movements, and the production of knowledge in the middle and later decades of the eighteenth century. I argue, however, for the relevance of the georgic to earlier poems written in response to the consequences of the English civil wars. The dissertation includes chapters devoted separately to Marvell, Finch, and Dryden, and concludes with a chapter on how their dynamic conceptions of georgic peace both inform and conflict with aspects of the popular eighteenth-century genre of imitative georgic poetry initiated by Philips and brought to its height by James Thomson.</subfield></datafield><datafield ind1=" " ind2=" " tag="653"><subfield code="a">Literature</subfield></datafield><datafield ind1=" " ind2=" " tag="653"><subfield code="a">Eighteenth-century</subfield></datafield><datafield ind1=" " ind2=" " tag="653"><subfield code="a">Georgic</subfield></datafield><datafield ind1=" " ind2=" " tag="653"><subfield code="a">Peace</subfield></datafield><datafield ind1=" " ind2=" " tag="653"><subfield code="a">Poetry</subfield></datafield><datafield ind1=" " ind2=" " tag="653"><subfield code="a">Restoration</subfield></datafield><datafield ind1=" " ind2=" " tag="653"><subfield code="a">Virgil</subfield></datafield><datafield ind1=" 7" ind2=" " tag="924">            <subfield code="a">oai:open.bu.edu:2144/15687</subfield>            <subfield code="c">Undetermined</subfield>            <subfield code="2">OpenBU</subfield>            <subfield code="0">http://hdl.handle.net/2144/15687</subfield>        </datafield></record></bib>'
        headers = {  'Content-Type':'application/xml'  }
        request = Request(url + queryParams
        , data=values
        , headers=headers)
        request.get_method = lambda: 'POST'
        ## uncomment to actually create the record in Alma
        response_body = urlopen(request).read()

        #return(values)
        return(response_body)
    
    @staticmethod
    def make_field(d,subfields):
        '''paramter d is a dictionary carrying the values for the marc field
        parameter subfields is a list of dicts carrying the values for the subfields'''
        if len(subfields) > 0:
            f = ET.Element('datafield')
            f.attrib['tag'] = d['tag']
            f.attrib['ind1'] = d['ind1']
            f.attrib['ind2'] = d['ind2']
            for sub in subfields:
                s = ET.Element('subfield')
                s.attrib['code'] = sub['code']
                s.text = sub['text']
                f.append(s)
        elif len(subfields)==0:
            f = ET.Element('controlfield')
            f.attrib['tag'] = d['tag']
            f.attrib['ind1'] = d['ind1']
            f.attrib['ind2'] = d['ind2']
            f.text = d['text']
        else:
            print(len(subfields))
            print(subfields)
            pass
        return(f)
    
    def add_ht_collection(mms_id):
        #print(mms_id)
        #print(len(mms_id))
        try:
            url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/bibs/collections/{pid}/bibs'.replace('{pid}',quote_plus('81850954700001161')) # HathiTrust
            queryParams = '?' + urlencode({ quote_plus('apikey') : 'l7xxf2bd5e66223d4008b04eba8751a96856'  }) ## production
            #queryParams = '?' + urlencode({ quote_plus('apikey') : 'l7xx4f9174fae8bc49a780ba6d5a93151af0'  }) ## sandbox
            values  = '<bib><mms_id>{{mmsid}}</mms_id></bib>'.replace('{{mmsid}}',mms_id)
            values = values.encode()
            headers = {  'Content-Type':'application/xml'  }
            request = Request(url + queryParams
            , data=values
            , headers=headers)
            request.get_method = lambda: 'POST'
            response_body = urlopen(request).read()
            return(response_body)
        except Exception as e:
            print(values)
            print(e)
            reponse_body = ''
        return(response_body)
    
    def add_ia_collection(mms_id):
        #print(mms_id)
        #print(len(mms_id))
        try:
            url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/bibs/collections/{pid}/bibs'.replace('{pid}',quote_plus('81870588400001161')) # InternetArchive
            queryParams = '?' + urlencode({ quote_plus('apikey') : 'l7xxf2bd5e66223d4008b04eba8751a96856'  }) ## production
            #queryParams = '?' + urlencode({ quote_plus('apikey') : 'l7xx4f9174fae8bc49a780ba6d5a93151af0'  }) ## sandbox
            values  = '<bib><mms_id>{{mmsid}}</mms_id></bib>'.replace('{{mmsid}}',mms_id)
            values = values.encode()
            headers = {  'Content-Type':'application/xml'  }
            request = Request(url + queryParams
            , data=values
            , headers=headers)
            request.get_method = lambda: 'POST'
            response_body = urlopen(request).read()
        except Exception as e:
            print(values)
            print(e)
            reponse_body = ''
        return(response_body)
class Primo:

    
    '''
    Primo is a set of tools to search Primo records
    '''
    def __init__(self):
        pass
        return
    def build_url(search_string,bulkSize):
        '''
        Function: build_url

        Purpose: Returns properly formatted url for a search string passed as the search_string parameter.
                 The url is built using the following variables defined in the class:
                 url_base
                 institution
                 bulkSize
                 onCampus
                 scope

         Parameter:  search_string
                     This is typically passed from a list of search strings

        '''
        url_base = 'http://bu-primo.hosted.exlibrisgroup.com/PrimoWebServices/xservice/search/brief'
        query_Params1 = '?institution=BOSU&query=any,contains,'
        query_Params2 = '&indx=1&bulkSize=' + bulkSize
        query_Params3 = '&loc=local,scope:(ALMA_BOSU1)&loc=adaptor,primo_central_multiple_fe&onCampus=true&json=true'
        url = url_base+query_Params1 + quote_plus(search_string.replace('  ',' ')) + query_Params2 + query_Params3
        return(url)

    def get_primo_results(url):
        '''get_primo_results executes the search and returns the response'''
        request = Request(url)
        try:
            response_body = urlopen(request).read()
        except Exception as e:
            response_body = ''

        return(response_body)

    def get_primo_json(json_str):
        '''get_primo_json parses the primo result string'''
        total_hits = json_str['SEGMENTS']['JAGROOT']['RESULT']['DOCSET']['@TOTALHITS']
        if total_hits == '0':
            return(total_hits)
            #return('none')
        if int(total_hits) == 1:
            #print(total_hits)
            num_recs = 1
            sourceID = json_str['SEGMENTS']['JAGROOT']['RESULT']['DOCSET']['DOC']['PrimoNMBib']['record']['control']['sourceid']
            delCat = json_str['SEGMENTS']['JAGROOT']['RESULT']['DOCSET']['DOC']['PrimoNMBib']['record']['delivery']['delcategory']
            try:
                recordid = json_str['SEGMENTS']['JAGROOT']['RESULT']['DOCSET']['DOC']['PrimoNMBib']['record']['search']['addsrcrecordid'] #.keys()
            except KeyError as e:
                recordid = json_str['SEGMENTS']['JAGROOT']['RESULT']['DOCSET']['DOC']['PrimoNMBib']['record']['search']['recordid'] #.keys()
    #        almaMMS_ID = json_str['SEGMENTS']['JAGROOT']['RESULT']['DOCSET']['DOC']['PrimoNMBib']['record']['search']['addsrcrecordid'] #.keys()
            title = json_str['SEGMENTS']['JAGROOT']['RESULT']['DOCSET']['DOC']['PrimoNMBib']['record']['search']['title']
            creationDate = json_str['SEGMENTS']['JAGROOT']['RESULT']['DOCSET']['DOC']['PrimoNMBib']['record']['search']['creationdate']#.keys()
            creators = json_str['SEGMENTS']['JAGROOT']['RESULT']['DOCSET']['DOC']['PrimoNMBib']['record']['search']['creatorcontrib']
            #print(num_recs)
            #print(sourceID)
            #print(delCat)
            #print(recordid)
            #print(title)
            #print(creators)
            #print(creationDate)
            #print()
        else:
            try:
                num_recs = len(json_str['SEGMENTS']['JAGROOT']['RESULT']['DOCSET']['DOC'])
            except Exception as e:
                print(e)
                print(total_recs)

            for i in range(0,num_recs):

                sourceID = json_str['SEGMENTS']['JAGROOT']['RESULT']['DOCSET']['DOC'][i]['PrimoNMBib']['record']['control']['sourceid']
                delCat = json_str['SEGMENTS']['JAGROOT']['RESULT']['DOCSET']['DOC'][i]['PrimoNMBib']['record']['delivery']['delcategory']
                try:
                    recordid = json_str['SEGMENTS']['JAGROOT']['RESULT']['DOCSET']['DOC'][i]['PrimoNMBib']['record']['search']['addsrcrecordid'] #.keys()
                except KeyError as e:
                    recordid = json_str['SEGMENTS']['JAGROOT']['RESULT']['DOCSET']['DOC'][i]['PrimoNMBib']['record']['search']['recordid'] #.keys()


                title = json_str['SEGMENTS']['JAGROOT']['RESULT']['DOCSET']['DOC'][i]['PrimoNMBib']['record']['search']['title']
                creationDate = json_str['SEGMENTS']['JAGROOT']['RESULT']['DOCSET']['DOC'][i]['PrimoNMBib']['record']['search']['creationdate']#.keys()
                creators = json_str['SEGMENTS']['JAGROOT']['RESULT']['DOCSET']['DOC'][i]['PrimoNMBib']['record']['search']['creatorcontrib']
                #print(num_recs)
                #print(sourceID)
                #print(delCat)
                #print(almaMMS_ID)
                #print(title)
                #print(creators)
                #print(creationDate)
                #print()
        return(num_recs,sourceID,delCat,recordid,title,creators,creationDate)
    
class Dspace:
    '''
    a set of tools to obtain records from Dspace
    '''
    def __init__(self):
        pass
        return

    def get_openBU_results(identifier,rights):
        '''get_primo_results executes the search and returns the response'''
        url ='http://open.bu.edu/oai/request?verb=GetRecord&identifier={{identifier}}&metadataPrefix=marc'
        url = url.replace('{{identifier}}',quote_plus(identifier))
        #print(url)
        request = Request(url)
        #rights = 0
        recno=identifier
        if type(rights) != str:
            rights = 'Undetermined'
            #rights = 'cc-by-nc-sa-4.0'
        rights_dict = {'pd':'public domain','pdus':'public domain (US)','icworld':'in copyright (world)',
                   'icus':'in copyright (US)','ic':'in copyright','und':'unknown',
                   'cc-by-nc-nd-3.0': 'Creative Commons Attribution-NonCommercial-NoDerivatives',
                   'cc-by-nc-nd-4.0': 'Creative Commons Attribution-NonCommercial-NoDerivatives',
                   'cc-by-nc-3.0': 'Creative Commons Attribution-NonCommercial',
                   'cc-by-nc-4.0': 'Creative Commons Attribution-NonCommercial',
                   'cc-by-nc-sa-4.0': 'Creative Commons Attribution-NonCommercial-ShareAlike',
                   'cc-by-nc-sa-3.0': 'Creative Commons Attribution-NonCommercial-ShareAlike',
                   'cc-by-sa-4.0': 'Creative Commons Attribution-ShareAlike',
                   'cc-by-sa-3.0': 'Creative Commons Attribution-ShareAlike',
                   'cc-by-3.0': 'Creative Commons Attribution',
                   'cc-by-4.0': 'Creative Commons Attribution',
                   }

        ## now let's add the ht_recno to 024$a
        if rights in rights_dict:
            rights = rights_dict[rights]

        _024 = '''       <datafield ind1="7" ind2=" " tag="024">
                <subfield code="a">{{OpenBU_record}}</subfield>
                <subfield code="c">{{rights}}</subfield>
                <subfield code="2">OpenBU</subfield>
                <subfield code="0">http://hdl.handle.net/{{record}}</subfield>
            </datafield>'''.replace('{{OpenBU_record}}',recno).replace('{{rights}}',rights).replace('{{record}}',recno[16:])
        _924 = '''       <datafield ind1="7" ind2=" " tag="924">
                <subfield code="a">{{OpenBU_record}}</subfield>
                <subfield code="c">{{rights}}</subfield>
                <subfield code="2">OpenBU</subfield>
                <subfield code="0">http://hdl.handle.net/{{record}}</subfield>
            </datafield>'''.replace('{{OpenBU_record}}',recno).replace('{{rights}}',rights).replace('{{record}}',recno[16:])

        field_924 = ET.fromstring(_924)
        field_024 = ET.fromstring(_024)

        try:
            
            response_body = urlopen(request).read()
            oai_result = ET.fromstring(response_body)
            #ns = {'oai':'http://www.openarchives.org/OAI/2.0/','marc':'http://www.loc.gov/MARC21/slim'}
            
            ns ={'oai':'http://www.openarchives.org/OAI/2.0/','marc':'http://www.loc.gov/MARC21/slim','ns0':'http://www.openarchives.org/OAI/2.0/'}
            header = oai_result.find('./ns0:GetRecord/ns0:record/ns0:header',ns)
            try:
                if header.attrib['status'] == 'deleted':
                    return('deleted','')
            except Exception as e:
                pass

            oai_header = oai_result.find('oai:GetRecord',ns).getchildren()[0][0]
            metadata = oai_result.find('oai:GetRecord',ns).getchildren()[0][1]
            marc_record = metadata.find('marc:record',ns)
            _024s = marc_record.findall('*/[@tag="024"]')
            for _024 in _024s:
                marc_record.remove(_024)
            marc_record.append(field_024)
            marc_record.append(field_924)

            for el in marc_record.findall('*/[@tag="720"]'):
                for e in el.getchildren():
                #print(e.tag,e.attrib,e.text)
                    if e.text == 'author':
                        el.attrib = {'tag':'100', 'ind1': '1', 'ind2': ' '}

            #marc_record = sort_marc_tags(marc_record)
        except Exception as e:
            print('There was an exception')
            print(e)
            oai_header = ''
            marc_record = ''
        #print(ET.tostring(marc_record))
        return(oai_header,marc_record)




    
