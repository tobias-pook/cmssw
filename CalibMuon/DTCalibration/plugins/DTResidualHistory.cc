
/*
 *  See header file for a description of this class.
 *
 */

#include "DTResidualHistory.h"

// Framework
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/EventSetup.h"
#include "FWCore/Framework/interface/ESHandle.h"
#include "FWCore/Utilities/interface/InputTag.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"

//Geometry
#include "Geometry/DTGeometry/interface/DTGeometry.h"
#include "Geometry/Records/interface/MuonGeometryRecord.h"

//RecHit
#include "DataFormats/DTRecHit/interface/DTRecSegment4DCollection.h"
#include "DataFormats/DTRecHit/interface/DTRecHitCollection.h"

#include "CommonTools/Utils/interface/TH1AddDirectorySentry.h"
#include "CalibMuon/DTCalibration/interface/DTSegmentSelector.h"
#include "CalibMuon/DTCalibration/interface/DTRecHitSegmentResidual.h"

#include "TFile.h"
#include "TH1F.h"
#include "TH2F.h"

#include <algorithm>

using namespace std;

DTResidualHistory::DTResidualHistory(const edm::ParameterSet& pset):
  select_(pset),
  segment4DLabel_(pset.getParameter<edm::InputTag>("segment4DLabel")),
  rootBaseDir_(pset.getUntrackedParameter<std::string>("rootBaseDir","DT/Residuals")),
  detailedAnalysis_(pset.getUntrackedParameter<bool>("detailedAnalysis",false)) {

  edm::LogVerbatim("Calibration") << "[DTResidualHistory] Constructor called.";
  consumes< DTRecSegment4DCollection >(edm::InputTag(segment4DLabel_));
  std::string rootFileName = pset.getUntrackedParameter<std::string>("rootFileName","residuals.root");
  rootFile_ = new TFile(rootFileName.c_str(), "RECREATE");
  rootFile_->cd();
  
  segmok=0;
  segmbad=0;
  nevent=0;
}

DTResidualHistory::~DTResidualHistory() {
  edm::LogVerbatim("Calibration") << "[DTResidualHistory] Destructor called.";
  cout << endl << " Finished. " << endl;
  cout << " Analyzed events: " << nevent << endl;
  cout << " Good segments: " << segmok << endl;
  cout << " Bad segments: " << segmbad << endl;
}

void DTResidualHistory::beginJob() {
//  TH1::SetDefaultSumw2(true);
}

void DTResidualHistory::beginRun(const edm::Run& run, const edm::EventSetup& setup) {
  
  // get the geometry
  edm::ESHandle<DTGeometry> dtGeomH; 
  setup.get<MuonGeometryRecord>().get(dtGeomH);
  dtGeom_ = dtGeomH.product();
  lastrun = 0;
}

void DTResidualHistory::analyze(const edm::Event& event, const edm::EventSetup& setup) {

  const DTSuperLayerId slIdToFill(-2,1,1,1);

  rootFile_->cd();
  ++nevent;
  unsigned int run = event.id().run();
  if (run!=lastrun) {
    TH1AddDirectorySentry addDir;
    histoMapTH1F_.clear();
    for (auto ch_it : dtGeom_->chambers()) {
      // Loop over the SLs
      for (auto sl_it : ch_it->superLayers()) {
        DTSuperLayerId slId = (sl_it)->id();
        bookHistos(slId,run);
        if(detailedAnalysis_) {
          for (auto layer_it : (sl_it)->layers()) {
            DTLayerId layerId = (layer_it)->id();
            bookHistos(layerId,run);
          }
        }
      }
    }

    std::string runStr = "Run" + std::to_string(run);
    TDirectory* baseDir = rootFile_->GetDirectory(runStr.c_str());
    if(!baseDir) baseDir = rootFile_->mkdir(runStr.c_str());
    baseDir->cd();
    histoResLs = new TH2F("histoResLs","Residuals vs Lumisection",100,0,10000,100,-1,1);
    std::vector<TH2F*> histosTH2F;
    histosTH2F.push_back(histoResLs);
    histoMapTH2F_[slIdToFill] = histosTH2F;
    rootFile_->cd();
  
    lastrun=run;
  }

  // Get the 4D rechits from the event
  edm::Handle<DTRecSegment4DCollection> segment4Ds;
  event.getByLabel(segment4DLabel_, segment4Ds);
 
  // Loop over segments by chamber
  DTRecSegment4DCollection::id_iterator chamberIdIt;
  for(chamberIdIt = segment4Ds->id_begin(); chamberIdIt != segment4Ds->id_end(); ++chamberIdIt){

     // Get the range for the corresponding ChamberId
     DTRecSegment4DCollection::range range = segment4Ds->get((*chamberIdIt));
     // Loop over the rechits of this DetUnit
     for(DTRecSegment4DCollection::const_iterator segment  = range.first;
                                                  segment != range.second; ++segment){

        if( !select_(*segment, event, setup) ) { segmbad++; continue; }
        segmok++;

        // Get all 1D RecHits at step 3 within the 4D segment
        std::vector<DTRecHit1D> recHits1D_S3;
  
        if( (*segment).hasPhi() ){
           const DTChamberRecSegment2D* phiSeg = (*segment).phiSegment();
           const std::vector<DTRecHit1D>& phiRecHits = phiSeg->specificRecHits();
           std::copy(phiRecHits.begin(), phiRecHits.end(), back_inserter(recHits1D_S3));
        }

        if( (*segment).hasZed() ){
           const DTSLRecSegment2D* zSeg = (*segment).zSegment();
           const std::vector<DTRecHit1D>& zRecHits = zSeg->specificRecHits();
           std::copy(zRecHits.begin(), zRecHits.end(), back_inserter(recHits1D_S3));
        }

        // Loop over 1D RecHit inside 4D segment
        for(std::vector<DTRecHit1D>::const_iterator recHit1D = recHits1D_S3.begin();
                                                    recHit1D != recHits1D_S3.end(); ++recHit1D) {
           const DTWireId wireId = recHit1D->wireId();
           float residualOnDistance = DTRecHitSegmentResidual().compute(dtGeom_,*recHit1D,*segment);
           fillHistos(wireId.superlayerId(), residualOnDistance);
           if (wireId.superlayerId() == slIdToFill) histoResLs->Fill(event.id().luminosityBlock(),residualOnDistance);
           if(detailedAnalysis_) fillHistos(wireId.layerId(), residualOnDistance);
        }
     }
  }
}

float DTResidualHistory::segmentToWireDistance(const DTRecHit1D& recHit1D, const DTRecSegment4D& segment){

  // Get the layer and the wire position
  const DTWireId wireId = recHit1D.wireId();
  const DTLayer* layer = dtGeom_->layer(wireId);
  float wireX = layer->specificTopology().wirePosition(wireId.wire());
      
  // Extrapolate the segment to the z of the wire
  // Get wire position in chamber RF
  // (y and z must be those of the hit to be coherent in the transf. of RF in case of rotations of the layer alignment)
  LocalPoint wirePosInLay(wireX,recHit1D.localPosition().y(),recHit1D.localPosition().z());
  GlobalPoint wirePosGlob = layer->toGlobal(wirePosInLay);
  const DTChamber* chamber = dtGeom_->chamber(wireId.layerId().chamberId());
  LocalPoint wirePosInChamber = chamber->toLocal(wirePosGlob);
      
  // Segment position at Wire z in chamber local frame
  LocalPoint segPosAtZWire = segment.localPosition()	+ segment.localDirection()*wirePosInChamber.z()/cos(segment.localDirection().theta());
      
  // Compute the distance of the segment from the wire
  int sl = wireId.superlayer();
  float segmDistance = -1;
  if(sl == 1 || sl == 3) segmDistance = fabs(wirePosInChamber.x() - segPosAtZWire.x());
  else if(sl == 2)       segmDistance =  fabs(segPosAtZWire.y() - wirePosInChamber.y());
     
  return segmDistance;
}

void DTResidualHistory::endJob(){
  
  edm::LogVerbatim("Calibration") << "[DTResidualHistory] Writing histos to file.";
  rootFile_->cd();
  rootFile_->Write();
  rootFile_->Close();

}

void DTResidualHistory::bookHistos(DTSuperLayerId slId, unsigned int run) {
  TH1AddDirectorySentry addDir;
  rootFile_->cd();

  cout << "[DTResidualHistory] Booking histos for SL: " << slId << endl;

  // Compose the chamber name
  std::string runStr = "Run" + std::to_string(run);
  std::string wheelStr = std::to_string(slId.wheel());
  // Define the step
  int step = 3;

  std::string slHistoName =
    "_STEP" + std::to_string(step) +
    "_W" + wheelStr +
    "_St" + std::to_string(slId.station()) +
    "_Sec" + std::to_string(slId.sector()) +
    "_SL" + std::to_string(slId.superlayer());

  TDirectory* baseDir = rootFile_->GetDirectory((runStr).c_str());
  if(!baseDir) baseDir = rootFile_->mkdir((runStr).c_str());

  TDirectory* wheelDir = baseDir->GetDirectory(("Wheel" + wheelStr).c_str());
  if(!wheelDir) wheelDir = baseDir->mkdir(("Wheel" + wheelStr).c_str());

  wheelDir->cd();
  // Create the monitor elements
  std::vector<TH1F*> histosTH1F;
  histosTH1F.push_back(new TH1F(("hRes"+slHistoName).c_str(),
                                 "Residuals on the dist. (cm) from wire (rec_hit - segm_extr)",
                                 200, -1., 1.));
  histoMapTH1F_[slId] = histosTH1F;
  
}

void DTResidualHistory::bookHistos(DTLayerId layerId, unsigned int run) {
  TH1AddDirectorySentry addDir;
  rootFile_->cd();

  cout << "[DTResidualHistory] Booking histos for layer: " << layerId << endl;

  // Compose the chamber name
  std::string runStr = "Run" + std::to_string(run);
  std::string wheelStr = std::to_string(layerId.wheel());
  // Define the step
  int step = 3;

  std::string layerHistoName =
    "_STEP" + std::to_string(step) +
    "_W" + wheelStr +
    "_St" + std::to_string(layerId.station()) +
    "_Sec" + std::to_string(layerId.sector()) +
    "_SL" + std::to_string(layerId.superlayer()) +
    "_Layer" + std::to_string(layerId.layer());

  TDirectory* baseDir = rootFile_->GetDirectory((runStr).c_str());
  if(!baseDir) baseDir = rootFile_->mkdir((runStr).c_str());

  TDirectory* wheelDir = baseDir->GetDirectory(("Wheel" + wheelStr).c_str());
  if(!wheelDir) wheelDir = baseDir->mkdir(("Wheel" + wheelStr).c_str());

  wheelDir->cd();

  // Create histograms
  std::vector<TH1F*> histosTH1F;
  histosTH1F.push_back(new TH1F(("hRes"+layerHistoName).c_str(),
                                 "Residuals on the dist. (cm) from wire (rec_hit - segm_extr)",
                                 200, -1., 1.));
  histoMapPerLayerTH1F_[layerId] = histosTH1F;
}

// Fill a set of histograms for a given SL 
void DTResidualHistory::fillHistos(DTSuperLayerId slId, float residualOnDistance) {
  std::vector<TH1F*> const& histosTH1F = histoMapTH1F_[slId];
  histosTH1F[0]->Fill(residualOnDistance);
}

// Fill a set of histograms for a given layer 
void DTResidualHistory::fillHistos(DTLayerId layerId, float residualOnDistance) {
  std::vector<TH1F*> const& histosTH1F = histoMapPerLayerTH1F_[layerId];
  histosTH1F[0]->Fill(residualOnDistance);
}

