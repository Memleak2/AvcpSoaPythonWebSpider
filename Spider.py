import urllib2
from bs4 import BeautifulSoup
import threading
import re
import requests
from pattern.web import URL
from urllib2 import Request
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfdevice import PDFDevice
from pdfminer.layout import LAParams
from pdfminer.converter import  HTMLConverter # , XMLConverter, HTMLConverter
from pymongo import MongoClient
import datetime
import sys


class WebSpider(threading.Thread):
    def __init__(self,htmlPage,region):
        self.startPage = htmlPage
        self.Region = region
    
    def getPdfText(self,pdfUrl):
        open = urllib2.urlopen(Request(pdfUrl)).read()
        from StringIO import StringIO
        memory_file = StringIO(open)
        parser = PDFParser(memory_file)
        document = PDFDocument(parser)
        rsrcmgr = PDFResourceManager()
        retstr = StringIO()
        laparams = LAParams()
        codec = 'utf-8'
        device = HTMLConverter(rsrcmgr, retstr, codec = codec, laparams = laparams)
        interpreter = PDFPageInterpreter(rsrcmgr, device)
        for page in PDFPage.create_pages(document):
            interpreter.process_page(page)
            data =  retstr.getvalue()
        
        return data
    
    def GetData(self):

        db = MongoClient()
        collection = db.local.Soa
        log = db.local.Log
        count = 0
        print("Grabbing data for "+ self.Region)
        log.insert_one({'type':'info','message' : 'Analisi: '+self.Region+' cominciata alle '+str(datetime.datetime.now())})

        districtPage = BeautifulSoup(self.startPage, 'html.parser')
        for companyLink in districtPage.find('tbody').findAll('a'):
            try:

                companyPage =  BeautifulSoup(requests.get(mainUrl + companyLink.get('href'),cookies=cookie).text, 'html.parser')
                entity = {
                          "codiceFiscale" : str(companyPage.find(string='Codice Fiscale Azienda').next_element.text.strip().decode('utf-8')),
                          "indirizzo" : str(companyPage.find(string='Indirizzo').next_element.text.strip().decode('utf-8')),
                          "denominazione" : str(companyPage.find(string='Denominazione').next_element.text.replace('\"','').strip().decode('utf-8')),
                          "cap" : str(companyPage.find(string='CAP').next_element.text.strip().decode('utf-8')),
                          "piva" : str(companyPage.find(string='Partita IVA').next_element.text.strip().decode('utf-8')),
                          "citta" : str(companyPage.find(string='Partita IVA').next_element.next_element.next_element.next_sibling.text.strip().decode('utf-8')),
                          "regione": str(self.Region),
                          "nazione" : str(companyPage.find(string='Nazione').next_element.text.strip().decode('utf-8'))
                          }

                detailsNode = companyPage.find(string='Ultima Attestazione:').parent.parent.findAll('td')

                if re.match("[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]",str(detailsNode[0].text.replace('\n','').strip().decode('utf-8'))) :
                    entity["dataRilascio"] = datetime.datetime.strptime(str(detailsNode[0].text.replace('\n','').strip().decode('utf-8')), "%d/%m/%Y")
                else:
                    entity["dataRilascio"] = ''

                entity["codiceSoa"]= str(detailsNode[1].text.strip().decode('utf-8'))
                entity["numeroAttestazione"] = str(detailsNode[2].text.strip().decode('utf-8'))
                entity["regolamento"] = str(detailsNode[3].text.strip().decode('utf-8'))
                entity["linkAttestato"] = str(detailsNode[4].find('a').get('href').replace("/Attestazioni14Portlet/../portal/RicercaAttestazioni/","https://servizi.avcp.it/portal/RicercaAttestazioni/"))

                infosParser = BeautifulSoup(self.getPdfText(entity["linkAttestato"]), 'html.parser')                            

                proceed = False

                if infosParser.find(string = re.compile('Date')) != None:
                    if re.match("[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]",infosParser.find(string=re.compile('Date')).parent.parent.next_sibling.text.replace('\n','').strip()) : 
                       proceed = True
                       entity["rilascioAttestazioneOriginaria"] = datetime.datetime.strptime(infosParser.find(string=re.compile('Date')).parent.parent.next_sibling.text.replace('\n','').strip(), "%d/%m/%Y")
                    elif re.match("[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]",infosParser.find(string=re.compile('Date')).parent.parent.next_sibling.next_sibling.text.replace('\n','').strip()):
                        proceed = True
                        entity["rilascioAttestazioneOriginaria"] = datetime.datetime.strptime(infosParser.find(string=re.compile('Date')).parent.parent.next_sibling.next_sibling.text.replace('\n','').strip(), "%d/%m/%Y")

                if proceed:

                    if entity['denominazione'] == '':
                        entity['denominazione'] = infosParser.find(string = re.compile('Rilasciato alla impresa')).parent.parent.next_sibling.text

                    entity['piva'] = str(infosParser.find(string = re.compile('IVA')).parent.parent.next_sibling.text.replace('\n','').strip())

                    if infosParser.find(string=re.compile('scadenza validit')).parent.parent.next_sibling != None and re.match("[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]",infosParser.find(string=re.compile('scadenza validit')).parent.parent.next_sibling.text.replace('\n','').strip()) :
                        entity["scadenzaValiditaTriennale"] = datetime.datetime.strptime(infosParser.find(string=re.compile('scadenza validit')).parent.parent.next_sibling.text.replace('\n','').strip(), "%d/%m/%Y")
                    else: 
                        entity["scadenzaValiditaTriennale"] =''

                    if infosParser.find(string=re.compile('rilascio attestazione in corso')).parent.parent.next_sibling != None and re.match("[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]",infosParser.find(string=re.compile('rilascio attestazione in corso')).parent.parent.next_sibling.text.replace('\n','').strip()) :
                        entity["rilascioAttestazioneInCorso"] = datetime.datetime.strptime(infosParser.find(string=re.compile('rilascio attestazione in corso')).parent.parent.next_sibling.text.replace('\n','').strip(), "%d/%m/%Y")
                    else :
                        entity["rilascioAttestazioneInCorso"] = ''

                    if infosParser.find(string='triennale\n').parent.parent.next_sibling != None and re.match("[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]",infosParser.find(string='triennale\n').parent.parent.next_sibling.text.replace('\n','').strip()) :
                        entity["effetuazioneVerificaTriennale"] = datetime.datetime.strptime(infosParser.find(string='triennale\n').parent.parent.next_sibling.text.replace('\n','').strip(), "%d/%m/%Y") 

                    if infosParser.find(string=re.compile('quinquennale')).parent.parent.next_sibling != None and re.match("[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]",infosParser.find(string=re.compile('quinquennale')).parent.parent.next_sibling.text.replace('\n','').strip()) :
                        entity["scadenzaValiditaQuinquennale"] = datetime.datetime.strptime(infosParser.find(string=re.compile('quinquennale')).parent.parent.next_sibling.text.replace('\n','').strip(), "%d/%m/%Y")
                    else:
                        entity["scadenzaValiditaQuinquennale"] = ''

                    if infosParser.find(string=re.compile('stab.')).parent.parent.next_sibling != None and re.match("[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]",infosParser.find(string=re.compile('stab.')).parent.parent.next_sibling.text.replace('\n','').strip()) :
                        entity["scadenzaIntermedia"] = datetime.datetime.strptime(infosParser.find(string=re.compile('stab.')).parent.parent.next_sibling.text.replace('\n','').strip(), "%d/%m/%Y")
                    else:
                        entity["scadenzaIntermedia"] = ''

                else:
                    entity["rilascioAttestazioneOriginaria"] = 'Non leggibile'
                    entity["scadenzaValiditaTriennale"] = 'Non leggibile'
                    entity["rilascioAttestazioneInCorso"] = 'Non leggibile'
                    entity["effetuazioneVerificaTriennale"] = 'Non leggibile'
                    entity["scadenzaValiditaQuinquennale"] = 'Non leggibile'
                    entity["scadenzaIntermedia"] = 'Non leggibile'
              
                stillExist = collection.find_one({'codiceFiscale':entity['codiceFiscale']},{'_id':1})

                if stillExist == None:
                   collection.insert_one(entity)
                   count +=1
                   print('1 new added for '+self.Region+' total: '+ str(count))
                else:
                   collection.update({'_id':stillExist['_id']}, {"$set": entity}, upsert=False)
                   print('Updated '+entity['codiceFiscale'])

            except  Exception as ex:
                    log.insert_one({'type':'error','args':ex.args,'date': datetime.datetime.now()})
        
        db.close()

# uncomment line for jumping some region from parsing
#jump = ('Abruzzo','Basilicata','Calabria','Campania')

#Passing region argument like 'Abruzzo' for scanning just 1 region
argument = sys.argv[1] if len(sys.argv) > 1 else ''

mainUrl = 'https://servizi.avcp.it'
response = requests.get(mainUrl+'/portal/classic/Servizi/RicercaAttestazioni')
cookie  = dict(JSESSIONID = response.cookies["JSESSIONID"])

mainPage = BeautifulSoup(response.text, 'html.parser')

if argument == '':
    for districtLink in mainPage.find_all(href=re.compile('isSecure=false')):
        try:
            if districtLink.text in jump:
                continue
            WebSpider(requests.get(mainUrl + districtLink.get('href'),cookies=cookie).text,districtLink.text).GetData()
        except Exception as ex:
            pass

else:
    link = mainPage.find(href=re.compile('isSecure=false'),string=re.compile(argument))
    WebSpider(requests.get(mainUrl + link.get('href'),cookies=cookie).text,link.text).GetData()
       
    
   