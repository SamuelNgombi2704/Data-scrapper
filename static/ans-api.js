/* ANS Annuaire SantÃ© FHIR connector controls. API keys stay in the worker environment. */
const baseSourcesPage=window.sourcesPage;
const baseProjectPage=window.projectPage;
window.projectPage=async function(){
  await baseProjectPage();
  state.sources.filter(source=>source.adapter==='ans-fhir').forEach(source=>{
    document.querySelectorAll('#app .source-line').forEach(row=>{
      if(row.textContent.includes(source.host)){
        const small=row.querySelector('small');if(small)small.textContent=source.host+' Â· ANS FHIR API Â· Key required in worker environment';
      }
    });
  });
};
window.sourcesPage=function(){
  baseSourcesPage();
  const actions=document.querySelector('#app .page-heading .heading-actions');
  if(actions){const button=document.createElement('button');button.className='button secondary';button.textContent='ï¼‹ Add ANS directory';button.onclick=addAnsSource;actions.append(button)}
  state.sources.filter(source=>source.adapter==='ans-fhir').forEach(source=>{
    document.querySelectorAll('#app .source-line').forEach(row=>{
      if(row.textContent.includes(source.host)){
        const badge=row.querySelector('.pill.green');if(badge)badge.textContent='ANS FHIR API';
        const small=row.querySelector('small');if(small)small.textContent=source.host+' Â· Official FHIR API Â· Key required in worker environment';
      }
    });
  });
};
async function addAnsSource(){
  const due=new Date();due.setFullYear(due.getFullYear()+1);
  const policyReviewDue=prompt('Next ANS data reuse policy review date (YYYY-MM-DD):',due.toISOString().slice(0,10));
  if(!policyReviewDue)return;
  if(!confirm('Confirm you reviewed the ANS Annuaire SantÃ© reuse terms and will collect only the selected public professional fields?'))return;
  try{
    await api('/api/sources',{method:'POST',body:JSON.stringify({adapter:'ans-fhir',name:'Annuaire SantÃ© (ANS FHIR)',policyReviewDue,termsReviewed:true})});
    toast('ANS source added. Add ANS_FHIR_API_KEY to the worker environment before running it.');
    await renderRoute();
  }catch(error){toast(error.message,true)}
}
const baseConfigureSource=window.configureSource;
window.configureSource=async function(projectId,sourceId=null){
  const all=await api('/api/projects/'+projectId+'/sources');
  const candidates=state.sources.filter(source=>!all.some(link=>link.source_id===source.id)||source.id===sourceId);
  let source=sourceId?state.sources.find(item=>item.id===sourceId):null;
  if(!source){
    if(!candidates.length)return toast('All available sources are already linked.',true);
    const choice=prompt('Choose a source by number:\n'+candidates.map((item,i)=>(i+1)+'. '+item.name+' ('+item.adapter+')').join('\n'),'1');
    if(choice===null)return;
    source=candidates[Number(choice)-1];
  }
  if(!source)return toast('Choose an available source.',true);
  if(source.adapter!=='ans-fhir')return baseConfigureSource(projectId,source.id);
  const linked=all.find(item=>item.source_id===source.id);
  const fields=prompt('ANS fields to save (comma-separated): name, professional_id, role, specialty, company, location, professional_url. Optional practice_address, practice_phone and practice_email come only from linked Organization records; secure health messaging addresses are excluded.',linked?JSON.parse(linked.selected_fields).join(', '):'name, professional_id, role, specialty, company, location, professional_url');
  if(fields===null)return;
  const searchType=prompt('Filter type: name, identifier (RPPS), or qualification-code',linked?(JSON.parse(linked.query_json||'{}').identifier?'identifier':JSON.parse(linked.query_json||'{}')['qualification-code']?'qualification-code':'name'):'name');
  if(!searchType)return;
  if(!['name','identifier','qualification-code'].includes(searchType))return toast('Choose name, identifier, or qualification-code.',true);
  const oldQuery=linked?JSON.parse(linked.query_json||'{}'):{};
  const value=prompt(searchType==='identifier'?'RPPS identifier':searchType==='qualification-code'?'ANS qualification code (for example SM41)':'Professional name',oldQuery[searchType]||'');
  if(!value)return;
  try{
    await api('/api/projects/'+projectId+'/sources/'+source.id,{method:'PUT',body:JSON.stringify({allowedPaths:['/fhir/v2/Practitioner','/fhir/v2/Organization','/fhir/v2/_page'],selectedFields:fields.split(',').map(x=>x.trim()).filter(Boolean),pacingSeconds:3,query:{[searchType]:value,active:'true'}})});
    toast('ANS filter and fields saved. The key is configured separately on the worker.');
    await projectPage();
  }catch(error){toast(error.message,true)}
};
const baseReviewSource=window.reviewSource;
window.reviewSource=async function(id){
  const source=state.sources.find(item=>item.id===id);
  if(!source||source.adapter!=='ans-fhir')return baseReviewSource(id);
  const termsUrl=source.terms_url||'https://esante.gouv.fr/sites/default/files/media/document/Charte_d_utilisation_des_donnees_RPPS.pdf';
  if(!confirm('Confirm you reviewed the ANS data reuse terms at '+termsUrl+' and still have authorization to use these public fields?'))return;
  const due=prompt('Next policy review due (YYYY-MM-DD):',source.policy_review_due||'');
  if(!due)return;
  try{
    await api('/api/sources/'+id+'/policy-review',{method:'POST',body:JSON.stringify({termsUrl,policyReviewDue:due,termsReviewed:true})});
    toast('ANS terms review recorded.');await renderRoute();
  }catch(error){toast(error.message,true)}
};
const baseScheduleRun=window.scheduleRun;
window.scheduleRun=async function(projectId){
  const links=await api('/api/projects/'+projectId+'/sources');
  if(!links.length)return baseScheduleRun(projectId);
  const ansLinks=links.filter(link=>state.sources.some(item=>item.id===link.source_id&&item.adapter==='ans-fhir'));
  if(!ansLinks.length)return baseScheduleRun(projectId);
  let selectedLink=ansLinks[0];
  if(ansLinks.length>1){
    const choice=prompt('Choose an ANS source by number:\n'+ansLinks.map((link,i)=>(i+1)+'. '+state.sources.find(item=>item.id===link.source_id).name).join('\n'),'1');
    if(choice===null)return;
    selectedLink=ansLinks[Number(choice)-1];if(!selectedLink)return toast('Choose a valid ANS source.',true);
  }
  const source=state.sources.find(item=>item.id===selectedLink.source_id);
  const query=JSON.parse(selectedLink.query_json||'{}');
  const when=prompt('Schedule time in your local time (YYYY-MM-DDTHH:mm), or leave blank to queue now:',new Date(Date.now()+3600000).toISOString().slice(0,16));
  if(when===null)return;
  const key=crypto.randomUUID();
  try{
    const result=await api('/api/projects/'+projectId+'/runs',{method:'POST',headers:{'Idempotency-Key':key},body:JSON.stringify({sourceId:source.id,search:query,idempotencyKey:key,scheduledAt:when?new Date(when).toISOString():null})});
    toast(result.job.status==='queued'?'ANS query queued. The worker will use its local API key.':'ANS query scheduled and added to the agenda.');
    await renderRoute();
  }catch(error){toast(error.message,true)}
};
