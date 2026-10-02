"""Derive read routes from the current application's Relay operation metadata."""
import base64

EXCLUDED = {
    'ConfigureAnalyticsQuery': 'Analytics configuration is not a recorded play result.',
    'useShareMyOutfitQuery': 'Creates a share rendering; recorded outfit data is fetched separately.',
    'VsHistoryDetailPagerRefetchQuery': (
        'Its seven selected fields are a strict subset of VsHistoryDetailQuery; '
        'the full detail response already preserves them.'
    ),
}

def walk(value, path=()):
    yield path, value
    if isinstance(value, dict):
        for k, v in value.items(): yield from walk(v, path+(k,))
    elif isinstance(value, list):
        for k, v in enumerate(value): yield from walk(v, path+(k,))

def decoded_id(value):
    if not isinstance(value,str): return ''
    try: return base64.b64decode(value+'='*(-len(value)%4)).decode('utf8')
    except (ValueError, UnicodeError): return value

def identity(remote):
    decoded=decoded_id(remote)
    if decoded.startswith('VsHistoryDetail-'):
        parts=decoded[len('VsHistoryDetail-'):].split(':')
        return 'vs',parts[0]+':'+parts[-1]
    if decoded.startswith('CoopHistoryDetail-'):return 'coop',decoded[len('CoopHistoryDetail-'):]
    return None

def fields(selections):
    for s in selections:
        if s['kind'] in ('Condition','InlineFragment','ClientExtension','Defer','Stream'):
            yield from fields(s.get('selections',[]))
        elif s['kind'] in ('LinkedField','ScalarField'):yield s

class Planner:
    def __init__(self, manifest):
        self.manifest=manifest
        self.queries=manifest['queries']
        self.routes={};self.excluded={};self.unsupported={}
        for name,q in self.queries.items():
            if q['params']['operationKind']!='query':
                self.excluded[name]='Mutation: changes server state, not a record read.';continue
            if name in EXCLUDED:self.excluded[name]=EXCLUDED[name];continue
            args={a['name']:a.get('defaultValue') for a in q['operation']['argumentDefinitions']}
            bindings={}
            for f in fields(q['operation']['selections']):
                for a in f.get('args') or []:
                    if a['kind']=='Variable' and a['name']=='id':
                        types=[x['type'] for x in f.get('selections',[]) if x['kind']=='InlineFragment']
                        if f.get('concreteType'):types.append(f['concreteType'])
                        if types:bindings[a['variableName']]=types
            if name=='DownloadSearchReplayQuery':bindings['code']=['Replay']
            known={'cursor','first','region','naCountry','fetchCurrentPlayer','fetchEquipments','isRegular','isBankara','isXBattle','isEvent','isPrivate','page','pageAr','pageCl','pageGl','pageLf'}
            unknown=set(args)-known-set(bindings)
            if unknown:self.unsupported[name]='Unbound variables: '+','.join(sorted(unknown));continue
            self.routes[name]={'args':args,'bindings':bindings,'query':q}

    def defaults(self,name,country='JP'):
        v=self.routes[name]['args'].copy()
        for k in v:
            if k=='naCountry':v[k]=country
            elif k.startswith('fetch') or k.startswith('is'):v[k]=True
            elif k=='first' and not v[k]:v[k]=25
        return v

    def roots(self,country):
        for n,r in self.routes.items():
            if not r['bindings']:yield n,self.defaults(n,country)

    def related(self,typename,obj,country):
        for n,r in self.routes.items():
            if len(r['bindings'])!=1:continue
            var,types=next(iter(r['bindings'].items()))
            if typename not in types:continue
            if var=='code':
                raw=obj.get('replayCode')
                val=''.join(c for c in raw if not (c.isspace() or c=='-')).upper() if isinstance(raw,str) else None
            else:
                val=obj.get('id')
            if not val:continue
            variables=self.defaults(n,country);variables[var]=val
            yield n,variables

    def visit(self,name,data,variables):
        """Yield schema-typed objects and continuations with exact response aliases."""
        def descend(selections,obj,path=(),typename=None):
            if not isinstance(obj,dict):return
            typ=obj.get('__typename') or typename
            if obj.get('id') and not typ:typ=decoded_id(obj['id']).split('-')[0]
            if typ:yield ('entity',typ,obj,path)
            for s in fields(selections):
                if s['kind']!='LinkedField':continue
                key=s.get('alias') or s['name'];val=obj.get(key)
                vals=enumerate(val) if isinstance(val,list) else [(None,val)]
                for index,child in vals:
                    if not isinstance(child,dict):continue
                    p=path+(key,)+(() if index is None else (index,))
                    pi=child.get('pageInfo',{})
                    av={a['name']:a['variableName'] for a in s.get('args') or [] if a['kind']=='Variable'}
                    if pi.get('hasNextPage') is True and 'after' in av:
                        cursor=pi.get('endCursor')
                        if not cursor or cursor==variables.get(av['after']):yield ('issue','PAGINATION_STALLED',p,None)
                        else:
                            nxt=variables.copy();nxt[av['after']]=cursor;yield ('next',name,nxt,p)
                    elif 'page' in av and 'after' not in av:
                        # Page-number APIs can lack pageInfo. Continue until an empty page;
                        # repeated page fingerprints are detected in the collector.
                        nodes=child.get('nodes',child.get('edges',[]))
                        if (pi.get('hasNextPage') is True) or ('hasNextPage' not in pi and nodes):
                            nxt=variables.copy();nxt[av['page']]=variables.get(av['page'],1)+1;yield ('page',name,nxt,(p,child))
                    yield from descend(s.get('selections',[]),child,p,s.get('concreteType'))
        yield from descend(self.queries[name]['operation']['selections'],data)

    def missing_fields(self,name,data,variables):
        """Check fields selected by the live operation, respecting fragments/conditions."""
        missing=[]
        def check(selections,obj,path=(),hint=None):
            if not isinstance(obj,dict):return
            typ=obj.get('__typename') or hint
            if not typ and obj.get('id'):typ=decoded_id(obj['id']).split('-')[0]
            for s in selections:
                k=s['kind']
                if k=='Condition':
                    if bool(variables.get(s['condition']))==s['passingValue']:check(s['selections'],obj,path,typ)
                elif k=='InlineFragment':
                    if s.get('abstractKey') or s['type']==typ:check(s['selections'],obj,path,typ)
                elif k in ('ScalarField','LinkedField'):
                    key=s.get('alias') or s['name']
                    if key not in obj:missing.append(path+(key,));continue
                    val=obj[key]
                    if k=='LinkedField':
                        for i,v in (enumerate(val) if isinstance(val,list) else [(None,val)]):
                            check(s['selections'],v,path+(key,)+(() if i is None else (i,)),s.get('concreteType'))
                elif k not in ('LinkedHandle','ScalarHandle','TypeDiscriminator','ClientExtension'):
                    missing.append(path+('__unhandled_selection__'+k,))
        check(self.queries[name]['operation']['selections'],data)
        return missing

    def invalid_shapes(self,name,data,variables):
        """Check that selected linked fields have Relay's singular/plural shape."""
        if not isinstance(data,dict):return []
        invalid=[]
        def check(selections,obj,path=(),hint=None):
            if not isinstance(obj,dict):return
            typ=obj.get('__typename') or hint
            if not typ and obj.get('id'):typ=decoded_id(obj['id']).split('-')[0]
            for s in selections:
                kind=s['kind']
                if kind=='Condition':
                    if bool(variables.get(s['condition']))==s['passingValue']:
                        check(s['selections'],obj,path,typ)
                elif kind=='InlineFragment':
                    if s.get('abstractKey') or s['type']==typ:
                        check(s['selections'],obj,path,typ)
                elif kind=='LinkedField':
                    key=s.get('alias') or s['name']
                    if key not in obj:
                        # Missing keys belong to missing_fields, avoiding duplicate issues.
                        continue
                    value=obj[key]
                    field_path=path+(key,)
                    if value is None:
                        continue
                    if s.get('plural') is True:
                        if not isinstance(value,list):
                            invalid.append(field_path)
                            continue
                        for index,item in enumerate(value):
                            item_path=field_path+(index,)
                            if item is None:
                                continue
                            if not isinstance(item,dict):
                                invalid.append(item_path)
                                continue
                            check(s.get('selections',[]),item,item_path,s.get('concreteType'))
                    else:
                        if not isinstance(value,dict):
                            invalid.append(field_path)
                            continue
                        check(s.get('selections',[]),value,field_path,s.get('concreteType'))
                elif kind in ('ScalarField','TypeDiscriminator','LinkedHandle','ScalarHandle','ClientExtension'):
                    continue
        check(self.queries[name]['operation']['selections'],data)
        return invalid
