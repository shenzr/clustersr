#include "Config.hh"

Config::Config(std::string confFile){
    XMLDocument doc; 
    doc.LoadFile(confFile.c_str());
    XMLElement* element;

    for(element = doc.FirstChildElement("setting")->FirstChildElement("attribute"); element!=NULL; element = element->NextSiblingElement("attribute")){
        
        XMLElement* ele = element->FirstChildElement("name");
        std::string attName = ele->GetText();
        if(attName == "erasure_code_k")
            _ecK = std::stoi(ele-> NextSiblingElement("value")-> GetText());

        else if (attName == "erasure_code_n") 
            _ecN = std::stoi(ele-> NextSiblingElement("value")-> GetText());
        
        else if (attName == "peer_node_num") 
            _peer_node_num = std::stoi(ele-> NextSiblingElement("value")-> GetText());

        else if (attName == "rack_num") 
            _rack_num = std::stoi(ele-> NextSiblingElement("value")-> GetText());

        else if (attName == "chunk_size")
            _chunk_size = std::stoi(ele-> NextSiblingElement("value")-> GetText());

        else if (attName == "meta_size")
            _meta_size = std::stoi(ele-> NextSiblingElement("value")-> GetText());

        else if (attName == "packet_size")
            _packet_size = std::stoi(ele-> NextSiblingElement("value")-> GetText());
        
        else if (attName == "stripe_num")
            _stripe_num = std::stoi(ele-> NextSiblingElement("value")-> GetText());
        
        else if (attName == "cross_rack_bandwidth")
            _cross_rack_bdwh = std::stod(ele-> NextSiblingElement("value")-> GetText());

        else if (attName == "intra_rack_bandwidth")
            _intra_rack_bdwh = std::stod(ele-> NextSiblingElement("value")-> GetText());

        else if (attName == "disk_bandwidth")
            _disk_bdwh = std::stod(ele-> NextSiblingElement("value")-> GetText());

        else if (attName == "balance_steps")
            _balance_steps = std::stod(ele-> NextSiblingElement("value")-> GetText());

        else if (attName == "balance_dev")
            _balance_dev = std::stod(ele-> NextSiblingElement("value")-> GetText());

        else if (attName == "local_data_path")
            _localDataPath = ele-> NextSiblingElement("value")-> GetText();

        else if (attName == "coordinator_ip")
            _coordinatorIP = inet_addr(ele-> NextSiblingElement("value")-> GetText());

        else if (attName == "peer_node_ips"){
            for(ele = ele->NextSiblingElement("value"); ele!=NULL; ele=ele->NextSiblingElement("value")){

                std::string tempstr = ele->GetText();
                int pos = tempstr.find("/");
                int len = tempstr.length();
                std::string ip = tempstr.substr(pos+1, len-pos-1);
                // std::cout << "Read IP: "<< ip << std::endl;
                // std::cout << "pos = " << pos << " len = "<< len << std::endl;
                // std::cout << "- %d " << inet_addr(ip.c_str()) << std::endl;
                _peerNodeIPs.push_back(inet_addr(ip.c_str()));
               
            }
            // sort(_peerNodeIPs.begin(), _peerNodeIPs.end()); 
        }
        else if (attName == "proxy_ips"){
            for(ele = ele->NextSiblingElement("value"); ele!=NULL; ele=ele->NextSiblingElement("value")){

                std::string tempstr = ele->GetText();
                int pos = tempstr.find("/");
                int len = tempstr.length();
                std::string ip = tempstr.substr(pos+1, len-pos-1);
                // std::cout << "Proxy IP: "<< ip << std::endl;
                // std::cout << "pos = " << pos << " len = "<< len << std::endl;
                // std::cout << "- %d " << inet_addr(ip.c_str()) << std::endl;
                _proxyIPs.push_back(inet_addr(ip.c_str()));
            }
            // sort(_peerNodeIPs.begin(), _peerNodeIPs.end()); 
        }
    }
}

void Config::display(){
    
    //std::cout << "Global info:"<< std::endl;
    //std::cout << "_ecK = "<< _ecK << std::endl;
    //std::cout << "_ecN = "<< _ecN << std::endl;
    //std::cout << "_chunk_size = "<< _chunk_size << std::endl;
    //std::cout << "_repair_method "<< _repair_method << std::endl;

}
