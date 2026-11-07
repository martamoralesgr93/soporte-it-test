var Jx=Object.defineProperty,e0=Object.defineProperties;var t0=Object.getOwnPropertyDescriptors;var mh=Object.getOwnPropertySymbols;var n0=Object.prototype.hasOwnProperty,i0=Object.prototype.propertyIsEnumerable;var uh=(t,n,e)=>n in t?Jx(t,n,{enumerable:!0,configurable:!0,writable:!0,value:e}):t[n]=e,P=(t,n)=>{for(var e in n||={})n0.call(n,e)&&uh(t,e,n[e]);if(mh)for(var e of mh(n))i0.call(n,e)&&uh(t,e,n[e]);return t},ge=(t,n)=>e0(t,t0(n));var Ja=[null],po=[!1],ui=1,a0=null,Ke=Symbol("SIGNAL");function j(t){let n=Ja[0];return Ja[0]=t,n}function bo(){return Ja[0]}var hi={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function qn(t){if(po[0])throw new Error("");let n=Ja[0];if(n===null)return;n.consumerOnSignalRead(t);let e=n.producersTail;if(e!==void 0&&e.producer===t)return;let i,a=n.recomputing;if(a&&(i=e!==void 0?e.nextProducer:n.producers,i!==void 0&&i.producer===t)){n.producersTail=i,i.lastReadVersion=t.version,i.knownValidAtEpoch=ui;return}let r=t.consumersTail;if(r!==void 0&&r.consumer===n&&(!a||r.knownValidAtEpoch===ui))return;let o=ra(n),s={producer:t,consumer:n,nextProducer:i,prevConsumer:void 0,knownValidAtEpoch:ui,lastReadVersion:t.version,nextConsumer:void 0};n.producersTail=s,e!==void 0?e.nextProducer=s:n.producers=s,o&&gh(t,s)}function ph(){ui++}function er(t){if(!(ra(t)&&!t.dirty)&&!(!t.dirty&&t.lastCleanEpoch===ui)){if(!t.producerMustRecompute(t)&&!aa(t)){go(t);return}t.producerRecomputeValue(t),go(t)}}function Il(t){if(t.consumers===void 0)return;let n=po[0];po[0]=!0;try{for(let e=t.consumers;e!==void 0;e=e.nextConsumer){let i=e.consumer;i.dirty||r0(i)}}finally{po[0]=n}}function Nl(){return Ja[0]?.consumerAllowSignalWrites!==!1}function r0(t){t.dirty=!0,Il(t),t.consumerMarkedDirty?.(t)}function go(t){t.dirty=!1,t.lastCleanEpoch=ui}function Yn(t){return t&&hh(t),j(t)}function hh(t){if(t.producersTail?.knownValidAtEpoch===ui){let n=t.producers;for(;n!==void 0;)n.knownValidAtEpoch=null,n=n.nextProducer}t.producersTail=void 0,t.recomputing=!0}function fi(t,n){j(n),t&&fh(t)}function fh(t){t.recomputing=!1;let n=t.producersTail,e=n!==void 0?n.nextProducer:t.producers;if(e!==void 0){if(ra(t))do e=Ml(e);while(e!==void 0);n!==void 0?n.nextProducer=void 0:t.producers=void 0}}function aa(t){for(let n=t.producers;n!==void 0;n=n.nextProducer){let e=n.producer,i=n.lastReadVersion;if(i!==e.version||(er(e),i!==e.version))return!0}return!1}function Zn(t){if(ra(t)){let n=t.producers;for(;n!==void 0;)n=Ml(n)}t.producers=void 0,t.producersTail=void 0,t.consumers=void 0,t.consumersTail=void 0}function gh(t,n){let e=t.consumersTail,i=ra(t);if(e!==void 0?(n.nextConsumer=e.nextConsumer,e.nextConsumer=n):(n.nextConsumer=void 0,t.consumers=n),n.prevConsumer=e,t.consumersTail=n,!i)for(let a=t.producers;a!==void 0;a=a.nextProducer)gh(a.producer,a)}function Ml(t){let n=t.producer,e=t.nextProducer,i=t.nextConsumer,a=t.prevConsumer;if(t.nextConsumer=void 0,t.prevConsumer=void 0,i!==void 0?i.prevConsumer=a:n.consumersTail=a,a!==void 0)a.nextConsumer=i;else if(n.consumers=i,!ra(n)){let r=n.producers;for(;r!==void 0;)r=Ml(r)}return e}function ra(t){return t.consumerIsAlwaysLive||t.consumers!==void 0}function vo(t){a0?.(t)}function _o(t,n){return Object.is(t,n)}function tr(t,n){let e=Object.create(o0);e.computation=t,n!==void 0&&(e.equal=n);let i=()=>{if(er(e),qn(e),e.value===pi)throw e.error;return e.value};return i[Ke]=e,vo(e),i}var ho=Symbol("UNSET"),fo=Symbol("COMPUTING"),pi=Symbol("ERRORED"),o0=ge(P({},hi),{value:ho,dirty:!0,error:null,equal:_o,kind:"computed",producerMustRecompute(t){return t.value===ho||t.value===fo},producerRecomputeValue(t){if(t.value===fo)throw new Error("");let n=t.value;t.value=fo;let e=Yn(t),i,a=!1;try{i=t.computation(),j(null),a=n!==ho&&n!==pi&&i!==pi&&t.equal(n,i)}catch(r){i=pi,t.error=r}finally{fi(t,e)}if(a){t.value=n;return}t.value=i,t.version++}});function s0(){throw new Error}var bh=s0;function vh(t){bh(t)}function Tl(t){bh=t}var c0=null;function Al(t,n){let e=Object.create(oa);e.value=t,n!==void 0&&(e.equal=n);let i=()=>_h(e);return i[Ke]=e,vo(e),[i,o=>gi(e,o),o=>Rl(e,o)]}function _h(t){return qn(t),t.value}function gi(t,n){Nl()||vh(t),t.equal(t.value,n)||(t.value=n,l0(t))}function Rl(t,n){Nl()||vh(t),gi(t,n(t.value))}var oa=ge(P({},hi),{equal:_o,value:void 0,kind:"signal"});function l0(t){t.version++,ph(),Il(t),c0?.(t)}var Ol=ge(P({},hi),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function Fl(t){if(t.dirty=!1,t.version>0&&!aa(t))return;t.version++;let n=Yn(t);try{t.cleanup(),t.fn()}finally{fi(t,n)}}var Ll=[void 0];function yo(){return Ll[0]}function ln(t){let n=Ll[0];return Ll[0]=t,n}var yh=Symbol("NotFound");function sa(t){return t===yh||t?.name==="\u0275NotFound"}function xh(t){let n=j(null);try{return t()}finally{j(n)}}function ue(t){return typeof t=="function"}function xo(t){let e=t(i=>{Error.call(i),i.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var wo=xo(t=>function(e){t(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((i,a)=>`${a+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function bi(t,n){if(t){let e=t.indexOf(n);0<=e&&t.splice(e,1)}}var pe=class t{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let r of e)r.remove(this);else e.remove(this);let{initialTeardown:i}=this;if(ue(i))try{i()}catch(r){n=r instanceof wo?r.errors:[r]}let{_finalizers:a}=this;if(a){this._finalizers=null;for(let r of a)try{wh(r)}catch(o){n=n??[],o instanceof wo?n=[...n,...o.errors]:n.push(o)}}if(n)throw new wo(n)}}add(n){var e;if(n&&n!==this)if(this.closed)wh(n);else{if(n instanceof t){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(n)}}_hasParent(n){let{_parentage:e}=this;return e===n||Array.isArray(e)&&e.includes(n)}_addParent(n){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(n),e):e?[e,n]:n}_removeParent(n){let{_parentage:e}=this;e===n?this._parentage=null:Array.isArray(e)&&bi(e,n)}remove(n){let{_finalizers:e}=this;e&&bi(e,n),n instanceof t&&n._removeParent(this)}};pe.EMPTY=(()=>{let t=new pe;return t.closed=!0,t})();var Pl=pe.EMPTY;function ko(t){return t instanceof pe||t&&"closed"in t&&ue(t.remove)&&ue(t.add)&&ue(t.unsubscribe)}function wh(t){ue(t)?t():t.unsubscribe()}var Yt={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var ca={setTimeout(t,n,...e){let{delegate:i}=ca;return i?.setTimeout?i.setTimeout(t,n,...e):setTimeout(t,n,...e)},clearTimeout(t){let{delegate:n}=ca;return(n?.clearTimeout||clearTimeout)(t)},delegate:void 0};function Co(t){ca.setTimeout(()=>{let{onUnhandledError:n}=Yt;if(n)n(t);else throw t})}function nr(){}var kh=Vl("C",void 0,void 0);function Ch(t){return Vl("E",void 0,t)}function Sh(t){return Vl("N",t,void 0)}function Vl(t,n,e){return{kind:t,value:n,error:e}}var vi=null;function la(t){if(Yt.useDeprecatedSynchronousErrorHandling){let n=!vi;if(n&&(vi={errorThrown:!1,error:null}),t(),n){let{errorThrown:e,error:i}=vi;if(vi=null,e)throw i}}else t()}function Eh(t){Yt.useDeprecatedSynchronousErrorHandling&&vi&&(vi.errorThrown=!0,vi.error=t)}var _i=class extends pe{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,ko(n)&&n.add(this)):this.destination=u0}static create(n,e,i){return new En(n,e,i)}next(n){this.isStopped?jl(Sh(n),this):this._next(n)}error(n){this.isStopped?jl(Ch(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?jl(kh,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},d0=Function.prototype.bind;function Bl(t,n){return d0.call(t,n)}var zl=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:e}=this;if(e.next)try{e.next(n)}catch(i){So(i)}}error(n){let{partialObserver:e}=this;if(e.error)try{e.error(n)}catch(i){So(i)}else So(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(e){So(e)}}},En=class extends _i{constructor(n,e,i){super();let a;if(ue(n)||!n)a={next:n??void 0,error:e??void 0,complete:i??void 0};else{let r;this&&Yt.useDeprecatedNextContext?(r=Object.create(n),r.unsubscribe=()=>this.unsubscribe(),a={next:n.next&&Bl(n.next,r),error:n.error&&Bl(n.error,r),complete:n.complete&&Bl(n.complete,r)}):a=n}this.destination=new zl(a)}};function So(t){Yt.useDeprecatedSynchronousErrorHandling?Eh(t):Co(t)}function m0(t){throw t}function jl(t,n){let{onStoppedNotification:e}=Yt;e&&ca.setTimeout(()=>e(t,n))}var u0={closed:!0,next:nr,error:m0,complete:nr};var da=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Zt(t){return t}function Dh(t){return t.length===0?Zt:t.length===1?t[0]:function(e){return t.reduce((i,a)=>a(i),e)}}var se=class t{constructor(n){n&&(this._subscribe=n)}lift(n){let e=new t;return e.source=this,e.operator=n,e}subscribe(n,e,i){let a=h0(n)?n:new En(n,e,i);return la(()=>{let{operator:r,source:o}=this;a.add(r?r.call(a,o):o?this._subscribe(a):this._trySubscribe(a))}),a}_trySubscribe(n){try{return this._subscribe(n)}catch(e){n.error(e)}}forEach(n,e){return e=Ih(e),new e((i,a)=>{let r=new En({next:o=>{try{n(o)}catch(s){a(s),r.unsubscribe()}},error:a,complete:i});this.subscribe(r)})}_subscribe(n){var e;return(e=this.source)===null||e===void 0?void 0:e.subscribe(n)}[da](){return this}pipe(...n){return Dh(n)(this)}toPromise(n){return n=Ih(n),new n((e,i)=>{let a;this.subscribe(r=>a=r,r=>i(r),()=>e(a))})}};se.create=t=>new se(t);function Ih(t){var n;return(n=t??Yt.Promise)!==null&&n!==void 0?n:Promise}function p0(t){return t&&ue(t.next)&&ue(t.error)&&ue(t.complete)}function h0(t){return t&&t instanceof _i||p0(t)&&ko(t)}function f0(t){return ue(t?.lift)}function we(t){return n=>{if(f0(n))return n.lift(function(e){try{return t(e,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function xe(t,n,e,i,a){return new Hl(t,n,e,i,a)}var Hl=class extends _i{constructor(n,e,i,a,r,o){super(n),this.onFinalize=r,this.shouldUnsubscribe=o,this._next=e?function(s){try{e(s)}catch(c){n.error(c)}}:super._next,this._error=a?function(s){try{a(s)}catch(c){n.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(s){n.error(s)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};var Nh=xo(t=>function(){t(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var E=class extends se{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(n){let e=new Eo(this,this);return e.operator=n,e}_throwIfClosed(){if(this.closed)throw new Nh}next(n){la(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let e of this.currentObservers)e.next(n)}})}error(n){la(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=n;let{observers:e}=this;for(;e.length;)e.shift().error(n)}})}complete(){la(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:n}=this;for(;n.length;)n.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var n;return((n=this.observers)===null||n===void 0?void 0:n.length)>0}_trySubscribe(n){return this._throwIfClosed(),super._trySubscribe(n)}_subscribe(n){return this._throwIfClosed(),this._checkFinalizedStatuses(n),this._innerSubscribe(n)}_innerSubscribe(n){let{hasError:e,isStopped:i,observers:a}=this;return e||i?Pl:(this.currentObservers=null,a.push(n),new pe(()=>{this.currentObservers=null,bi(a,n)}))}_checkFinalizedStatuses(n){let{hasError:e,thrownError:i,isStopped:a}=this;e?n.error(i):a&&n.complete()}asObservable(){let n=new se;return n.source=this,n}};E.create=(t,n)=>new Eo(t,n);var Eo=class extends E{constructor(n,e){super(),this.destination=n,this.source=e}next(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.next)===null||i===void 0||i.call(e,n)}error(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.error)===null||i===void 0||i.call(e,n)}complete(){var n,e;(e=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||e===void 0||e.call(n)}_subscribe(n){var e,i;return(i=(e=this.source)===null||e===void 0?void 0:e.subscribe(n))!==null&&i!==void 0?i:Pl}};var Kn=class extends E{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let e=super._subscribe(n);return!e.closed&&n.next(this._value),e}getValue(){let{hasError:n,thrownError:e,_value:i}=this;if(n)throw e;return this._throwIfClosed(),i}next(n){super.next(this._value=n)}};var ir={now(){return(ir.delegate||Date).now()},delegate:void 0};var Do=class extends E{constructor(n=1/0,e=1/0,i=ir){super(),this._bufferSize=n,this._windowTime=e,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,e)}next(n){let{isStopped:e,_buffer:i,_infiniteTimeWindow:a,_timestampProvider:r,_windowTime:o}=this;e||(i.push(n),!a&&i.push(r.now()+o)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(n),{_infiniteTimeWindow:i,_buffer:a}=this,r=a.slice();for(let o=0;o<r.length&&!n.closed;o+=i?1:2)n.next(r[o]);return this._checkFinalizedStatuses(n),e}_trimBuffer(){let{_bufferSize:n,_timestampProvider:e,_buffer:i,_infiniteTimeWindow:a}=this,r=(a?1:2)*n;if(n<1/0&&r<i.length&&i.splice(0,i.length-r),!a){let o=e.now(),s=0;for(let c=1;c<i.length&&i[c]<=o;c+=2)s=c;s&&i.splice(0,s+1)}}};var Io=class extends pe{constructor(n,e){super()}schedule(n,e=0){return this}};var ar={setInterval(t,n,...e){let{delegate:i}=ar;return i?.setInterval?i.setInterval(t,n,...e):setInterval(t,n,...e)},clearInterval(t){let{delegate:n}=ar;return(n?.clearInterval||clearInterval)(t)},delegate:void 0};var No=class extends Io{constructor(n,e){super(n,e),this.scheduler=n,this.work=e,this.pending=!1}schedule(n,e=0){var i;if(this.closed)return this;this.state=n;let a=this.id,r=this.scheduler;return a!=null&&(this.id=this.recycleAsyncId(r,a,e)),this.pending=!0,this.delay=e,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(r,this.id,e),this}requestAsyncId(n,e,i=0){return ar.setInterval(n.flush.bind(n,this),i)}recycleAsyncId(n,e,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return e;e!=null&&ar.clearInterval(e)}execute(n,e){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(n,e);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,e){let i=!1,a;try{this.work(n)}catch(r){i=!0,a=r||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),a}unsubscribe(){if(!this.closed){let{id:n,scheduler:e}=this,{actions:i}=e;this.work=this.state=this.scheduler=null,this.pending=!1,bi(i,this),n!=null&&(this.id=this.recycleAsyncId(e,n,null)),this.delay=null,super.unsubscribe()}}};var ma=class t{constructor(n,e=t.now){this.schedulerActionCtor=n,this.now=e}schedule(n,e=0,i){return new this.schedulerActionCtor(this,n).schedule(i,e)}};ma.now=ir.now;var Mo=class extends ma{constructor(n,e=ma.now){super(n,e),this.actions=[],this._active=!1}flush(n){let{actions:e}=this;if(this._active){e.push(n);return}let i;this._active=!0;do if(i=n.execute(n.state,n.delay))break;while(n=e.shift());if(this._active=!1,i){for(;n=e.shift();)n.unsubscribe();throw i}}};var rr=new Mo(No),Mh=rr;var Dn=new se(t=>t.complete());function To(t){return t&&ue(t.schedule)}function Ul(t){return t[t.length-1]}function Ao(t){return ue(Ul(t))?t.pop():void 0}function dn(t){return To(Ul(t))?t.pop():void 0}function Th(t,n){return typeof Ul(t)=="number"?t.pop():n}function Rh(t,n,e,i){function a(r){return r instanceof e?r:new e(function(o){o(r)})}return new(e||(e=Promise))(function(r,o){function s(d){try{l(i.next(d))}catch(u){o(u)}}function c(d){try{l(i.throw(d))}catch(u){o(u)}}function l(d){d.done?r(d.value):a(d.value).then(s,c)}l((i=i.apply(t,n||[])).next())})}function Ah(t){var n=typeof Symbol=="function"&&Symbol.iterator,e=n&&t[n],i=0;if(e)return e.call(t);if(t&&typeof t.length=="number")return{next:function(){return t&&i>=t.length&&(t=void 0),{value:t&&t[i++],done:!t}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function yi(t){return this instanceof yi?(this.v=t,this):new yi(t)}function Oh(t,n,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=e.apply(t,n||[]),a,r=[];return a=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),s("next"),s("throw"),s("return",o),a[Symbol.asyncIterator]=function(){return this},a;function o(f){return function(_){return Promise.resolve(_).then(f,u)}}function s(f,_){i[f]&&(a[f]=function(w){return new Promise(function(A,ie){r.push([f,w,A,ie])>1||c(f,w)})},_&&(a[f]=_(a[f])))}function c(f,_){try{l(i[f](_))}catch(w){g(r[0][3],w)}}function l(f){f.value instanceof yi?Promise.resolve(f.value.v).then(d,u):g(r[0][2],f)}function d(f){c("next",f)}function u(f){c("throw",f)}function g(f,_){f(_),r.shift(),r.length&&c(r[0][0],r[0][1])}}function Fh(t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=t[Symbol.asyncIterator],e;return n?n.call(t):(t=typeof Ah=="function"?Ah(t):t[Symbol.iterator](),e={},i("next"),i("throw"),i("return"),e[Symbol.asyncIterator]=function(){return this},e);function i(r){e[r]=t[r]&&function(o){return new Promise(function(s,c){o=t[r](o),a(s,c,o.done,o.value)})}}function a(r,o,s,c){Promise.resolve(c).then(function(l){r({value:l,done:s})},o)}}var Ro=(t=>t&&typeof t.length=="number"&&typeof t!="function");function Oo(t){return ue(t?.then)}function Fo(t){return ue(t[da])}function Lo(t){return Symbol.asyncIterator&&ue(t?.[Symbol.asyncIterator])}function Po(t){return new TypeError(`You provided ${t!==null&&typeof t=="object"?"an invalid object":`'${t}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function g0(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var Vo=g0();function Bo(t){return ue(t?.[Vo])}function jo(t){return Oh(this,arguments,function*(){let e=t.getReader();try{for(;;){let{value:i,done:a}=yield yi(e.read());if(a)return yield yi(void 0);yield yield yi(i)}}finally{e.releaseLock()}})}function zo(t){return ue(t?.getReader)}function Ve(t){if(t instanceof se)return t;if(t!=null){if(Fo(t))return b0(t);if(Ro(t))return v0(t);if(Oo(t))return _0(t);if(Lo(t))return Lh(t);if(Bo(t))return y0(t);if(zo(t))return x0(t)}throw Po(t)}function b0(t){return new se(n=>{let e=t[da]();if(ue(e.subscribe))return e.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function v0(t){return new se(n=>{for(let e=0;e<t.length&&!n.closed;e++)n.next(t[e]);n.complete()})}function _0(t){return new se(n=>{t.then(e=>{n.closed||(n.next(e),n.complete())},e=>n.error(e)).then(null,Co)})}function y0(t){return new se(n=>{for(let e of t)if(n.next(e),n.closed)return;n.complete()})}function Lh(t){return new se(n=>{w0(t,n).catch(e=>n.error(e))})}function x0(t){return Lh(jo(t))}function w0(t,n){var e,i,a,r;return Rh(this,void 0,void 0,function*(){try{for(e=Fh(t);i=yield e.next(),!i.done;){let o=i.value;if(n.next(o),n.closed)return}}catch(o){a={error:o}}finally{try{i&&!i.done&&(r=e.return)&&(yield r.call(e))}finally{if(a)throw a.error}}n.complete()})}function St(t,n,e,i=0,a=!1){let r=n.schedule(function(){e(),a?t.add(this.schedule(null,i)):this.unsubscribe()},i);if(t.add(r),!a)return r}function Ho(t,n=0){return we((e,i)=>{e.subscribe(xe(i,a=>St(i,t,()=>i.next(a),n),()=>St(i,t,()=>i.complete(),n),a=>St(i,t,()=>i.error(a),n)))})}function Uo(t,n=0){return we((e,i)=>{i.add(t.schedule(()=>e.subscribe(i),n))})}function Ph(t,n){return Ve(t).pipe(Uo(n),Ho(n))}function Vh(t,n){return Ve(t).pipe(Uo(n),Ho(n))}function Bh(t,n){return new se(e=>{let i=0;return n.schedule(function(){i===t.length?e.complete():(e.next(t[i++]),e.closed||this.schedule())})})}function jh(t,n){return new se(e=>{let i;return St(e,n,()=>{i=t[Vo](),St(e,n,()=>{let a,r;try{({value:a,done:r}=i.next())}catch(o){e.error(o);return}r?e.complete():e.next(a)},0,!0)}),()=>ue(i?.return)&&i.return()})}function $o(t,n){if(!t)throw new Error("Iterable cannot be null");return new se(e=>{St(e,n,()=>{let i=t[Symbol.asyncIterator]();St(e,n,()=>{i.next().then(a=>{a.done?e.complete():e.next(a.value)})},0,!0)})})}function zh(t,n){return $o(jo(t),n)}function Hh(t,n){if(t!=null){if(Fo(t))return Ph(t,n);if(Ro(t))return Bh(t,n);if(Oo(t))return Vh(t,n);if(Lo(t))return $o(t,n);if(Bo(t))return jh(t,n);if(zo(t))return zh(t,n)}throw Po(t)}function Bt(t,n){return n?Hh(t,n):Ve(t)}function In(...t){let n=dn(t);return Bt(t,n)}function Uh(t){return t instanceof Date&&!isNaN(t)}function Qe(t,n){return we((e,i)=>{let a=0;e.subscribe(xe(i,r=>{i.next(t.call(n,r,a++))}))})}var{isArray:k0}=Array;function C0(t,n){return k0(n)?t(...n):t(n)}function Go(t){return Qe(n=>C0(t,n))}var{isArray:S0}=Array,{getPrototypeOf:E0,prototype:D0,keys:I0}=Object;function Wo(t){if(t.length===1){let n=t[0];if(S0(n))return{args:n,keys:null};if(N0(n)){let e=I0(n);return{args:e.map(i=>n[i]),keys:e}}}return{args:t,keys:null}}function N0(t){return t&&typeof t=="object"&&E0(t)===D0}function qo(t,n){return t.reduce((e,i,a)=>(e[i]=n[a],e),{})}function $l(...t){let n=dn(t),e=Ao(t),{args:i,keys:a}=Wo(t);if(i.length===0)return Bt([],n);let r=new se(M0(i,n,a?o=>qo(a,o):Zt));return e?r.pipe(Go(e)):r}function M0(t,n,e=Zt){return i=>{$h(n,()=>{let{length:a}=t,r=new Array(a),o=a,s=a;for(let c=0;c<a;c++)$h(n,()=>{let l=Bt(t[c],n),d=!1;l.subscribe(xe(i,u=>{r[c]=u,d||(d=!0,s--),s||i.next(e(r.slice()))},()=>{--o||i.complete()}))},i)},i)}}function $h(t,n,e){t?St(e,t,n):n()}function Gh(t,n,e,i,a,r,o,s){let c=[],l=0,d=0,u=!1,g=()=>{u&&!c.length&&!l&&n.complete()},f=w=>l<i?_(w):c.push(w),_=w=>{r&&n.next(w),l++;let A=!1;Ve(e(w,d++)).subscribe(xe(n,ie=>{a?.(ie),r?f(ie):n.next(ie)},()=>{A=!0},void 0,()=>{if(A)try{for(l--;c.length&&l<i;){let ie=c.shift();o?St(n,o,()=>_(ie)):_(ie)}g()}catch(ie){n.error(ie)}}))};return t.subscribe(xe(n,f,()=>{u=!0,g()})),()=>{s?.()}}function Gl(t,n,e=1/0){return ue(n)?Gl((i,a)=>Qe((r,o)=>n(i,r,a,o))(Ve(t(i,a))),e):(typeof n=="number"&&(e=n),we((i,a)=>Gh(i,a,t,e)))}function Yo(t=1/0){return Gl(Zt,t)}function Wh(){return Yo(1)}function ua(...t){return Wh()(Bt(t,dn(t)))}function Wl(...t){let n=Ao(t),{args:e,keys:i}=Wo(t),a=new se(r=>{let{length:o}=e;if(!o){r.complete();return}let s=new Array(o),c=o,l=o;for(let d=0;d<o;d++){let u=!1;Ve(e[d]).subscribe(xe(r,g=>{u||(u=!0,l--),s[d]=g},()=>c--,void 0,()=>{(!c||!u)&&(l||r.next(i?qo(i,s):s),r.complete())}))}});return n?a.pipe(Go(n)):a}function or(t=0,n,e=Mh){let i=-1;return n!=null&&(To(n)?e=n:i=n),new se(a=>{let r=Uh(t)?+t-e.now():t;r<0&&(r=0);let o=0;return e.schedule(function(){a.closed||(a.next(o++),0<=i?this.schedule(void 0,i):a.complete())},r)})}function jt(...t){let n=dn(t),e=Th(t,1/0),i=t;return i.length?i.length===1?Ve(i[0]):Yo(e)(Bt(i,n)):Dn}function ot(t,n){return we((e,i)=>{let a=0;e.subscribe(xe(i,r=>t.call(n,r,a++)&&i.next(r)))})}function qh(t){return we((n,e)=>{let i=!1,a=null,r=null,o=!1,s=()=>{if(r?.unsubscribe(),r=null,i){i=!1;let l=a;a=null,e.next(l)}o&&e.complete()},c=()=>{r=null,o&&e.complete()};n.subscribe(xe(e,l=>{i=!0,a=l,r||Ve(t(l)).subscribe(r=xe(e,s,c))},()=>{o=!0,(!i||!r||r.closed)&&e.complete()}))})}function Zo(t,n=rr){return qh(()=>or(t,n))}function Nn(t,n=rr){return we((e,i)=>{let a=null,r=null,o=null,s=()=>{if(a){a.unsubscribe(),a=null;let l=r;r=null,i.next(l)}};function c(){let l=o+t,d=n.now();if(d<l){a=this.schedule(void 0,l-d),i.add(a);return}s()}e.subscribe(xe(i,l=>{r=l,o=n.now(),a||(a=n.schedule(c,t),i.add(a))},()=>{s(),i.complete()},void 0,()=>{r=a=null}))})}function ql(t){return t<=0?()=>Dn:we((n,e)=>{let i=0;n.subscribe(xe(e,a=>{++i<=t&&(e.next(a),t<=i&&e.complete())}))})}function Ko(t,n=Zt){return t=t??T0,we((e,i)=>{let a,r=!0;e.subscribe(xe(i,o=>{let s=n(o);(r||!t(a,s))&&(r=!1,a=s,i.next(o))}))})}function T0(t,n){return t===n}function Qo(){return we((t,n)=>{let e,i=!1;t.subscribe(xe(n,a=>{let r=e;e=a,i&&n.next([r,a]),i=!0}))})}function Yh(t={}){let{connector:n=()=>new E,resetOnError:e=!0,resetOnComplete:i=!0,resetOnRefCountZero:a=!0}=t;return r=>{let o,s,c,l=0,d=!1,u=!1,g=()=>{s?.unsubscribe(),s=void 0},f=()=>{g(),o=c=void 0,d=u=!1},_=()=>{let w=o;f(),w?.unsubscribe()};return we((w,A)=>{l++,!u&&!d&&g();let ie=c=c??n();A.add(()=>{l--,l===0&&!u&&!d&&(s=Yl(_,a))}),ie.subscribe(A),!o&&l>0&&(o=new En({next:fe=>ie.next(fe),error:fe=>{u=!0,g(),s=Yl(f,e,fe),ie.error(fe)},complete:()=>{d=!0,g(),s=Yl(f,i),ie.complete()}}),Ve(w).subscribe(o))})(r)}}function Yl(t,n,...e){if(n===!0){t();return}if(n===!1)return;let i=new En({next:()=>{i.unsubscribe(),t()}});return Ve(n(...e)).subscribe(i)}function Xo(t,n,e){let i,a=!1;return t&&typeof t=="object"?{bufferSize:i=1/0,windowTime:n=1/0,refCount:a=!1,scheduler:e}=t:i=t??1/0,Yh({connector:()=>new Do(i,n,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:a})}function xi(t){return ot((n,e)=>t<=e)}function yt(...t){let n=dn(t);return we((e,i)=>{(n?ua(t,e,n):ua(t,e)).subscribe(i)})}function pa(t,n){return we((e,i)=>{let a=null,r=0,o=!1,s=()=>o&&!a&&i.complete();e.subscribe(xe(i,c=>{a?.unsubscribe();let l=0,d=r++;Ve(t(c,d)).subscribe(a=xe(i,u=>i.next(n?n(c,u,d,l++):u),()=>{a=null,s()}))},()=>{o=!0,s()}))})}function Oe(t){return we((n,e)=>{Ve(t).subscribe(xe(e,()=>e.complete(),nr)),!e.closed&&n.subscribe(e)})}function Zl(t,n,e){let i=ue(t)||n||e?{next:t,error:n,complete:e}:t;return i?we((a,r)=>{var o;(o=i.subscribe)===null||o===void 0||o.call(i);let s=!0;a.subscribe(xe(r,c=>{var l;(l=i.next)===null||l===void 0||l.call(i,c),r.next(c)},()=>{var c;s=!1,(c=i.complete)===null||c===void 0||c.call(i),r.complete()},c=>{var l;s=!1,(l=i.error)===null||l===void 0||l.call(i,c),r.error(c)},()=>{var c,l;s&&((c=i.unsubscribe)===null||c===void 0||c.call(i)),(l=i.finalize)===null||l===void 0||l.call(i)}))}):Zt}var ld="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",H=class extends Error{code;constructor(n,e){super(ga(n,e)),this.code=n}};function A0(t){return`NG0${Math.abs(t)}`}function ga(t,n){return`${A0(t)}${n?": "+n:""}`}function Ee(t){for(let n in t)if(t[n]===Ee)return n;throw Error("")}function Jh(t,n){for(let e in n)Object.hasOwn(n,e)&&!Object.hasOwn(t,e)&&(t[e]=n[e])}function rs(t){if(typeof t=="string")return t;if(Array.isArray(t))return`[${t.map(rs).join(", ")}]`;if(t==null)return""+t;let n=t.overriddenName||t.name;if(n)return`${n}`;let e=t.toString();if(e==null)return""+e;let i=e.indexOf(`
`);return i>=0?e.slice(0,i):e}function os(t,n){return t?n?`${t} ${n}`:t:n||""}var R0=Ee({__forward_ref__:Ee});function ze(t){return t.__forward_ref__=ze,t}function st(t){return dd(t)?t():t}function dd(t){return typeof t=="function"&&Object.hasOwn(t,R0)&&t.__forward_ref__===ze}function Te(t){return{token:t.token,providedIn:t.providedIn||null,factory:t.factory,value:void 0}}function G(t){return{providers:t.providers||[],imports:t.imports||[]}}function ss(t){return O0(t,cs)}function O0(t,n){return Object.hasOwn(t,n)&&t[n]||null}function F0(t){let n=t?.[cs]??null;return n||null}function Ql(t){return t&&Object.hasOwn(t,es)?t[es]:null}var cs=Ee({\u0275prov:Ee}),es=Ee({\u0275inj:Ee}),y=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,e){this._desc=n,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=Te({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function md(t){return t&&!!t.\u0275providers}var ur=Ee({\u0275cmp:Ee}),pr=Ee({\u0275dir:Ee}),ud=Ee({\u0275pipe:Ee});var cr=Ee({\u0275fac:Ee}),Di=Ee({__NG_ELEMENT_ID__:Ee}),Zh=Ee({__NG_ENV_ID__:Ee});function Qn(t){return pd(t,"@Component"),t[ur]||null}function ls(t){return pd(t,"@Directive"),t[pr]||null}function ef(t){return pd(t,"@Pipe"),t[ud]||null}function pd(t,n){if(t==null)throw new H(-919,!1)}function ba(t){return typeof t=="string"?t:t==null?"":String(t)}var tf=Ee({ngErrorCode:Ee}),L0=Ee({ngErrorMessage:Ee}),P0=Ee({ngTokenPath:Ee});function hd(t,n){return nf("",-200,n)}function ds(t,n){throw new H(-201,!1)}function nf(t,n,e){let i=new H(n,t);return i[tf]=n,i[L0]=t,e&&(i[P0]=e),i}function V0(t){return t[tf]}var Xl;function af(){return Xl}function Tt(t){let n=Xl;return Xl=t,n}function fd(t,n,e){let i=ss(t);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(e&8)return null;if(n!==void 0)return n;ds(t,"")}var va=globalThis;var B0={},wi=B0,j0="__NG_DI_FLAG__",Jl=class{injector;constructor(n){this.injector=n}retrieve(n,e){let i=ki(e)||0;try{return this.injector.get(n,i&8?null:wi,i)}catch(a){if(sa(a))return a;throw a}}};function z0(t,n=0){let e=yo();if(e===void 0)throw new H(-203,!1);if(e===null)return fd(t,void 0,n);{let i=H0(n),a=e.retrieve(t,i);if(sa(a)){if(i.optional)return null;throw a}return a}}function ce(t,n=0){return(af()||z0)(st(t),n)}function m(t,n){return ce(t,ki(n))}function ki(t){return typeof t>"u"||typeof t=="number"?t:0|(t.optional&&8)|(t.host&&1)|(t.self&&2)|(t.skipSelf&&4)}function H0(t){return{optional:!!(t&8),host:!!(t&1),self:!!(t&2),skipSelf:!!(t&4)}}function ed(t){let n=[];for(let e=0;e<t.length;e++){let i=st(t[e]);if(Array.isArray(i)){if(i.length===0)throw new H(900,!1);let a,r=0;for(let o=0;o<i.length;o++){let s=i[o],c=U0(s);typeof c=="number"?c===-1?a=s.token:r|=c:a=s}n.push(ce(a,r))}else n.push(ce(i))}return n}function U0(t){return t[j0]}function Ci(t,n){let e=Object.hasOwn(t,cr);return e?t[cr]:null}function rf(t,n,e){if(t.length!==n.length)return!1;for(let i=0;i<t.length;i++){let a=t[i],r=n[i];if(e&&(a=e(a),r=e(r)),r!==a)return!1}return!0}function of(t){return t.flat(Number.POSITIVE_INFINITY)}function ms(t,n){t.forEach(e=>Array.isArray(e)?ms(e,n):n(e))}function gd(t,n,e){n>=t.length?t.push(e):t.splice(n,0,e)}function hr(t,n){return n>=t.length-1?t.pop():t.splice(n,1)[0]}function sf(t,n){let e=[];for(let i=0;i<t;i++)e.push(n);return e}function cf(t,n,e,i){let a=t.length;if(a==n)t.push(e,i);else if(a===1)t.push(i,t[0]),t[0]=e;else{for(a--,t.push(t[a-1],t[a]);a>n;){let r=a-2;t[a]=t[r],a--}t[n]=e,t[n+1]=i}}function us(t,n,e){let i=_a(t,n);return i>=0?t[i|1]=e:(i=~i,cf(t,i,n,e)),i}function ps(t,n){let e=_a(t,n);if(e>=0)return t[e|1]}function _a(t,n){return $0(t,n,1)}function $0(t,n,e){let i=0,a=t.length>>e;for(;a!==i;){let r=i+(a-i>>1),o=t[r<<e];if(n===o)return r<<e;o>n?a=r:i=r+1}return~(a<<e)}var Xn={},ft=[],Ii=new y(""),bd=new y("",-1),vd=new y(""),fa=class{get(n,e=wi){if(e===wi){let a=nf("",-201);throw a.name="\u0275NotFound",a}return e}};function Ni(t){return{\u0275providers:t}}function lf(t){return Ni([{provide:Ii,multi:!0,useValue:t}])}function df(...t){return{\u0275providers:_d(!0,t),\u0275fromNgModule:!0}}function _d(t,...n){let e=[],i=new Set,a,r=o=>{e.push(o)};return ms(n,o=>{let s=o;ts(s,r,[],i)&&(a||=[],a.push(s))}),a!==void 0&&mf(a,r),e}function mf(t,n){for(let e=0;e<t.length;e++){let{ngModule:i,providers:a}=t[e];yd(a,r=>{n(r,i)})}}function ts(t,n,e,i){if(t=st(t),!t)return!1;let a=null,r=Ql(t),o=!r&&Qn(t);if(!r&&!o){let c=t.ngModule;if(r=Ql(c),r)a=c;else return!1}else{if(o&&!o.standalone)return!1;a=t}let s=i.has(a);if(o){if(s)return!1;if(i.add(a),o.dependencies){let c=typeof o.dependencies=="function"?o.dependencies():o.dependencies;for(let l of c)ts(l,n,e,i)}}else if(r){if(r.imports!=null&&!s){i.add(a);let l;ms(r.imports,d=>{ts(d,n,e,i)&&(l||=[],l.push(d))}),l!==void 0&&mf(l,n)}if(!s){let l=Ci(a)||(()=>new a);n({provide:a,useFactory:l,deps:ft},a),n({provide:vd,useValue:a,multi:!0},a),n({provide:Ii,useValue:()=>ce(a),multi:!0},a)}let c=r.providers;if(c!=null&&!s){let l=t;yd(c,d=>{n(d,l)})}}else return!1;return a!==t&&t.providers!==void 0}function yd(t,n){for(let e of t)md(e)&&(e=e.\u0275providers),Array.isArray(e)?yd(e,n):n(e)}var G0=Ee({provide:String,useValue:Ee});function uf(t){return t!==null&&typeof t=="object"&&G0 in t}function W0(t){return!!(t&&t.useExisting)}function q0(t){return!!(t&&t.useFactory)}function Si(t){return typeof t=="function"}function pf(t){return!!t.useClass}var fr=new y(""),Jo={},Kh={},Kl;function ya(){return Kl===void 0&&(Kl=new fa),Kl}var ct=class{},Ei=class extends ct{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,e,i,a){super(),this.parent=e,this.source=i,this.scopes=a,nd(n,o=>this.processProvider(o)),this.records.set(bd,ha(void 0,this)),a.has("environment")&&this.records.set(ct,ha(void 0,this));let r=this.records.get(fr);r!=null&&typeof r.value=="string"&&this.scopes.add(r.value),this.injectorDefTypes=new Set(this.get(vd,ft,{self:!0}))}retrieve(n,e){let i=ki(e)||0;try{return this.get(n,wi,i)}catch(a){if(sa(a))return a;throw a}}destroy(){sr(this),this._destroyed=!0;let n=j(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of e)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),j(n)}}onDestroy(n){return sr(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){sr(this);let e=ln(this),i=Tt(void 0),a;try{return n()}finally{ln(e),Tt(i)}}get(n,e=wi,i){if(sr(this),Object.hasOwn(n,Zh))return n[Zh](this);let a=ki(i),r,o=ln(this),s=Tt(void 0);try{if(!(a&4)){let l=this.records.get(n);if(l===void 0){let d=X0(n)&&ss(n);d&&this.injectableDefInScope(d)?l=ha(td(n),Jo):l=null,this.records.set(n,l)}if(l!=null)return this.hydrate(n,l,a)}let c=a&2?ya():this.parent;return e=a&8&&e===wi?null:e,c.get(n,e)}catch(c){let l=V0(c);throw l===-200||l===-201?new H(l,null):c}finally{Tt(s),ln(o)}}resolveInjectorInitializers(){let n=j(null),e=ln(this),i=Tt(void 0),a;try{let r=this.get(Ii,ft,{self:!0});for(let o of r)o()}finally{ln(e),Tt(i),j(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=st(n);let e=Si(n)?n:st(n&&n.provide),i=Z0(n);if(!Si(n)&&n.multi===!0){let a=this.records.get(e);a||(a=ha(void 0,Jo,!0),a.factory=()=>ed(a.multi),this.records.set(e,a)),e=n,a.multi.push(n)}this.records.set(e,i)}hydrate(n,e,i){let a=j(null);try{if(e.value===Kh)throw hd("");return e.value===Jo&&(e.value=Kh,e.value=e.factory(void 0,i)),typeof e.value=="object"&&e.value&&Q0(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{j(a)}}injectableDefInScope(n){if(!n.providedIn)return!1;let e=st(n.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(n){let e=this._onDestroyHooks.indexOf(n);e!==-1&&this._onDestroyHooks.splice(e,1)}};function td(t){let n=ss(t),e=n!==null?n.factory:Ci(t);if(e!==null)return e;if(t instanceof y)throw new H(-204,!1);if(t instanceof Function)return Y0(t);throw new H(-204,!1)}function Y0(t){if(t.length>0)throw new H(-204,!1);let e=F0(t);return e!==null?()=>e.factory(t):()=>new t}function Z0(t){if(uf(t))return ha(void 0,t.useValue);{let n=xd(t);return ha(n,Jo)}}function xd(t,n,e){let i;if(Si(t)){let a=st(t);return Ci(a)||td(a)}else if(uf(t))i=()=>st(t.useValue);else if(q0(t))i=()=>t.useFactory(...ed(t.deps||[]));else if(W0(t))i=(a,r)=>ce(st(t.useExisting),r!==void 0&&r&8?8:void 0);else{let a=st(t&&(t.useClass||t.provide));if(K0(t))i=()=>new a(...ed(t.deps));else return Ci(a)||td(a)}return i}function sr(t){if(t.destroyed)throw new H(-205,!1)}function ha(t,n,e=!1){return{factory:t,value:n,multi:e?[]:void 0}}function K0(t){return!!t.deps}function Q0(t){return t!==null&&typeof t=="object"&&typeof t.ngOnDestroy=="function"}function X0(t){return typeof t=="function"||typeof t=="object"&&t.ngMetadataName==="InjectionToken"}function nd(t,n){for(let e of t)Array.isArray(e)?nd(e,n):e&&md(e)?nd(e.\u0275providers,n):n(e)}function hs(t,n){let e;t instanceof Ei?(sr(t),e=t):e=new Jl(t);let i,a=ln(e),r=Tt(void 0);try{return n()}finally{ln(a),Tt(r)}}function hf(){return af()!==void 0||yo()!=null}var Kt=0,W=1,Z=2,$e=3,zt=4,lt=5,Mi=6,xa=7,He=8,On=9,Qt=10,De=11,wa=12,wd=13,Jn=14,gt=15,ei=16,Ti=17,un=18,gr=19,kd=20,Mn=21,fs=22,Tn=23,At=24,Ai=25,ff=26,ka=27,Le=28,gf=1,Cd=6,Ri=7,br=8,Oi=9,Ue=10;function Fn(t){return Array.isArray(t)&&typeof t[gf]=="object"}function Ht(t){return Array.isArray(t)&&t[gf]===!0}function Sd(t){return(t.flags&4)!==0}function pn(t){return t.componentOffset>-1}function Ca(t){return(t.flags&1)===1}function hn(t){return!!t.template}function Sa(t){return(t[Z]&512)!==0}function Fi(t){return(t[Z]&256)===256}var Rt=(function(t){return t[t.NONE=0]="NONE",t[t.HTML=1]="HTML",t[t.STYLE=2]="STYLE",t[t.SCRIPT=3]="SCRIPT",t[t.URL=4]="URL",t[t.RESOURCE_URL=5]="RESOURCE_URL",t[t.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",t})(Rt||{});var Ed="svg",bf="math";function dt(t){for(;Array.isArray(t);)t=t[Kt];return t}function Dd(t,n){return dt(n[t])}function Ot(t,n){return dt(n[t.index])}function gs(t,n){return t.data[n]}function Id(t,n){return t[n]}function bs(t,n,e,i){e>=t.data.length&&(t.data[e]=null,t.blueprint[e]=null),n[e]=i}function Ut(t,n){let e=n[t];return Fn(e)?e:e[Kt]}function vf(t){return(t[Z]&4)===4}function vs(t){return(t[Z]&128)===128}function _f(t){return Ht(t[$e])}function Ft(t,n){return n==null?null:t[n]}function Nd(t){t[Ti]=0}function Md(t){t[Z]&1024||(t[Z]|=1024,vs(t)&&Li(t))}function yf(t,n){for(;t>0;)n=n[Jn],t--;return n}function vr(t){return!!(t[Z]&9216||t[At]?.dirty)}function _s(t){t[Qt].changeDetectionScheduler?.notify(8),t[Z]&64&&(t[Z]|=1024),vr(t)&&Li(t)}function Li(t){t[Qt].changeDetectionScheduler?.notify(0);let n=An(t);for(;n!==null&&!(n[Z]&8192||(n[Z]|=8192,!vs(n)));)n=An(n)}function ys(t,n){if(Fi(t))throw new H(911,!1);t[Mn]===null&&(t[Mn]=[]),t[Mn].push(n)}function xf(t,n){if(t[Mn]===null)return;let e=t[Mn].indexOf(n);e!==-1&&t[Mn].splice(e,1)}function An(t){let n=t[$e];return Ht(n)?n[$e]:n}function Td(t){return t[xa]??=[]}function Ad(t){return t.cleanup??=[]}function wf(t,n,e,i){let a=Td(n);a.push(e),t.firstCreatePass&&Ad(t).push(i,a.length-1)}var oe={lFrame:Rf(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var id=!1;function kf(){return oe.lFrame.elementDepthCount}function Cf(){oe.lFrame.elementDepthCount++}function Rd(){oe.lFrame.elementDepthCount--}function xs(){return oe.bindingsEnabled}function Od(){return oe.skipHydrationRootTNode!==null}function Fd(t){return oe.skipHydrationRootTNode===t}function Ld(){oe.skipHydrationRootTNode=null}function U(){return oe.lFrame.lView}function Ce(){return oe.lFrame.tView}function R(t){return oe.lFrame.contextLView=t,t[He]}function O(t){return oe.lFrame.contextLView=null,t}function Xe(){let t=Pd();for(;t!==null&&t.type===64;)t=t.parent;return t}function Pd(){return oe.lFrame.currentTNode}function Sf(){let t=oe.lFrame,n=t.currentTNode;return t.isParent?n:n.parent}function Pi(t,n){let e=oe.lFrame;e.currentTNode=t,e.isParent=n}function Vd(){return oe.lFrame.isParent}function Bd(){oe.lFrame.isParent=!1}function jd(){return oe.lFrame.contextLView}function zd(){return id}function lr(t){let n=id;return id=t,n}function Hd(){let t=oe.lFrame,n=t.bindingRootIndex;return n===-1&&(n=t.bindingRootIndex=t.tView.bindingStartIndex),n}function Ef(){return oe.lFrame.bindingIndex}function Df(t){return oe.lFrame.bindingIndex=t}function Ln(){return oe.lFrame.bindingIndex++}function ws(t){let n=oe.lFrame,e=n.bindingIndex;return n.bindingIndex=n.bindingIndex+t,e}function If(){return oe.lFrame.inI18n}function Nf(t,n){let e=oe.lFrame;e.bindingIndex=e.bindingRootIndex=t,ks(n)}function Mf(){return oe.lFrame.currentDirectiveIndex}function ks(t){oe.lFrame.currentDirectiveIndex=t}function Tf(t){let n=oe.lFrame.currentDirectiveIndex;return n===-1?null:t[n]}function Cs(){return oe.lFrame.currentQueryIndex}function _r(t){oe.lFrame.currentQueryIndex=t}function J0(t){let n=t[W];return n.type===2?n.declTNode:n.type===1?t[lt]:null}function Ud(t,n,e){if(e&4){let a=n,r=t;for(;a=a.parent,a===null&&!(e&1);)if(a=J0(r),a===null||(r=r[Jn],a.type&10))break;if(a===null)return!1;n=a,t=r}let i=oe.lFrame=Af();return i.currentTNode=n,i.lView=t,!0}function Ss(t){let n=Af(),e=t[W];oe.lFrame=n,n.currentTNode=e.firstChild,n.lView=t,n.tView=e,n.contextLView=t,n.bindingIndex=e.bindingStartIndex,n.inI18n=!1}function Af(){let t=oe.lFrame,n=t===null?null:t.child;return n===null?Rf(t):n}function Rf(t){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:t,child:null,inI18n:!1};return t!==null&&(t.child=n),n}function Of(){let t=oe.lFrame;return oe.lFrame=t.parent,t.currentTNode=null,t.lView=null,t}var $d=Of;function Es(){let t=Of();t.isParent=!0,t.tView=null,t.selectedIndex=-1,t.contextLView=null,t.elementDepthCount=0,t.currentDirectiveIndex=-1,t.currentNamespace=null,t.bindingRootIndex=-1,t.bindingIndex=-1,t.currentQueryIndex=0}function Ff(t){return(oe.lFrame.contextLView=yf(t,oe.lFrame.contextLView))[He]}function Xt(){return oe.lFrame.selectedIndex}function ti(t){oe.lFrame.selectedIndex=t}function Vi(){let t=oe.lFrame;return gs(t.tView,t.selectedIndex)}function Ge(){oe.lFrame.currentNamespace=Ed}function nt(){ew()}function ew(){oe.lFrame.currentNamespace=null}function Gd(){return oe.lFrame.currentNamespace}var Lf=!0;function Ds(){return Lf}function yr(t){Lf=t}function ad(t,n=null,e=null,i){let a=Pf(t,n,e,i);return a.resolveInjectorInitializers(),a}function Pf(t,n=null,e=null,i,a=new Set){let r=[e||ft,df(t)],o;return new Ei(r,n||ya(),o||null,a)}var X=class t{static THROW_IF_NOT_FOUND=wi;static NULL=new fa;static create(n,e){if(Array.isArray(n))return ad({name:""},e,n,"");{let i=n.name??"";return ad({name:i},n.parent,n.providers,i)}}static \u0275prov=Te({token:t,providedIn:"any",factory:()=>ce(bd)});static __NG_ELEMENT_ID__=-1},ee=new y(""),Et=class{static __NG_ELEMENT_ID__=tw;static __NG_ENV_ID__=n=>n},ns=class extends Et{_lView;constructor(n){super(),this._lView=n}get destroyed(){return Fi(this._lView)}onDestroy(n){let e=this._lView;return ys(e,n),()=>xf(e,n)}};function tw(){return new ns(U())}var Vf=!1,Bf=new y(""),Ea=(()=>{class t{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new Kn(!1);debugTaskTracker=m(Bf,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new se(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=Te({token:t,providedIn:"root",factory:()=>new t})}return t})(),rd=class extends E{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,hf()&&(this.destroyRef=m(Et,{optional:!0})??void 0,this.pendingTasks=m(Ea,{optional:!0})??void 0)}emit(n){let e=j(null);try{super.next(n)}finally{j(e)}}subscribe(n,e,i){let a=n,r=e||(()=>null),o=i;if(n&&typeof n=="object"){let c=n;a=c.next?.bind(c),r=c.error?.bind(c),o=c.complete?.bind(c)}this.__isAsync&&(r=this.wrapInTimeout(r),a&&(a=this.wrapInTimeout(a)),o&&(o=this.wrapInTimeout(o)));let s=super.subscribe({next:a,error:r,complete:o});return n instanceof pe&&n.add(s),s}wrapInTimeout(n){return e=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{n(e)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},z=rd;function is(...t){}function Wd(t){let n,e;function i(){t=is;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),n!==void 0&&clearTimeout(n)}catch{}}return n=setTimeout(()=>{t(),i()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{t(),i()})),()=>i()}function jf(t){return queueMicrotask(()=>t()),()=>{t=is}}var qd="isAngularZone",dr=qd+"_ID",nw=0,N=class t{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new z(!1);onMicrotaskEmpty=new z(!1);onStable=new z(!1);onError=new z(!1);constructor(n){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:a=!1,scheduleInRootZone:r=Vf}=n;if(typeof Zone>"u")throw new H(908,!1);Zone.assertZonePatched();let o=this;o._nesting=0,o._outer=o._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(o._inner=o._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(o._inner=o._inner.fork(Zone.longStackTraceZoneSpec)),o.shouldCoalesceEventChangeDetection=!a&&i,o.shouldCoalesceRunChangeDetection=a,o.callbackScheduled=!1,o.scheduleInRootZone=r,rw(o)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(qd)===!0}static assertInAngularZone(){if(!t.isInAngularZone())throw new H(909,!1)}static assertNotInAngularZone(){if(t.isInAngularZone())throw new H(909,!1)}run(n,e,i){return this._inner.run(n,e,i)}runTask(n,e,i,a){let r=this._inner,o=r.scheduleEventTask("NgZoneEvent: "+a,n,iw,is,is);try{return r.runTask(o,e,i)}finally{r.cancelTask(o)}}runGuarded(n,e,i){return this._inner.runGuarded(n,e,i)}runOutsideAngular(n){return this._outer.run(n)}},iw={};function Yd(t){if(t._nesting==0&&!t.hasPendingMicrotasks&&!t.isStable)try{t._nesting++,t.onMicrotaskEmpty.emit(null)}finally{if(t._nesting--,!t.hasPendingMicrotasks)try{t.runOutsideAngular(()=>t.onStable.emit(null))}finally{t.isStable=!0}}}function aw(t){if(t.isCheckStableRunning||t.callbackScheduled)return;t.callbackScheduled=!0;function n(){Wd(()=>{t.callbackScheduled=!1,od(t),t.isCheckStableRunning=!0,Yd(t),t.isCheckStableRunning=!1})}t.scheduleInRootZone?Zone.root.run(()=>{n()}):t._outer.run(()=>{n()}),od(t)}function rw(t){let n=()=>{aw(t)},e=nw++;t._inner=t._inner.fork({name:"angular",properties:{[qd]:!0,[dr]:e,[dr+e]:!0},onInvokeTask:(i,a,r,o,s,c)=>{if(ow(c))return i.invokeTask(r,o,s,c);try{return Qh(t),i.invokeTask(r,o,s,c)}finally{(t.shouldCoalesceEventChangeDetection&&o.type==="eventTask"||t.shouldCoalesceRunChangeDetection)&&n(),Xh(t)}},onInvoke:(i,a,r,o,s,c,l)=>{try{return Qh(t),i.invoke(r,o,s,c,l)}finally{t.shouldCoalesceRunChangeDetection&&!t.callbackScheduled&&!sw(c)&&n(),Xh(t)}},onHasTask:(i,a,r,o)=>{i.hasTask(r,o),a===r&&(o.change=="microTask"?(t._hasPendingMicrotasks=o.microTask,od(t),Yd(t)):o.change=="macroTask"&&(t.hasPendingMacrotasks=o.macroTask))},onHandleError:(i,a,r,o)=>(i.handleError(r,o),t.runOutsideAngular(()=>t.onError.emit(o)),!1)})}function od(t){t._hasPendingMicrotasks||(t.shouldCoalesceEventChangeDetection||t.shouldCoalesceRunChangeDetection)&&t.callbackScheduled===!0?t.hasPendingMicrotasks=!0:t.hasPendingMicrotasks=!1}function Qh(t){t._nesting++,t.isStable&&(t.isStable=!1,t.onUnstable.emit(null))}function Xh(t){t._nesting--,Yd(t)}var mr=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new z;onMicrotaskEmpty=new z;onStable=new z;onError=new z;run(n,e,i){return n.apply(e,i)}runGuarded(n,e,i){return n.apply(e,i)}runOutsideAngular(n){return n()}runTask(n,e,i,a){return n.apply(e,i)}};function ow(t){return zf(t,"__ignore_ng_zone__")}function sw(t){return zf(t,"__scheduler_tick__")}function zf(t,n){return!Array.isArray(t)||t.length!==1?!1:t[0]?.data?.[n]===!0}var mn=class{_console=console;handleError(n){this._console.error("ERROR",n)}};function Hf(t){return cw(t)?t:new sd(t)}function cw(t){return t instanceof Error||typeof t=="object"&&t!==null&&typeof t.name=="string"&&typeof t.message=="string"}var sd=class extends Error{constructor(n){super(String(n),{cause:n}),this.name="ErrorBoundaryWrappedError"}},ni=new y("",{factory:()=>{let t=m(N),n=m(ct),e;return i=>{t.runOutsideAngular(()=>{n.destroyed&&!e?setTimeout(()=>{throw i}):(e??=n.get(mn),e.handleError(i))})}}}),Uf={provide:Ii,useValue:()=>{let t=m(mn,{optional:!0})},multi:!0},lw=new y("",{factory:()=>{let t=m(ee).defaultView;if(!t)return;let n=m(ni),e=r=>{n(r.reason),r.preventDefault()},i=r=>{r.error?n(r.error):n(new Error(r.message,{cause:r})),r.preventDefault()},a=()=>{t.addEventListener("unhandledrejection",e),t.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(a):a(),m(Et).onDestroy(()=>{t.removeEventListener("error",i),t.removeEventListener("unhandledrejection",e)})}});function Zd(){return Ni([lf(()=>{m(lw)})])}function ae(t,n){let[e,i,a]=Al(t,n?.equal),r=e,o=r[Ke];return r.set=i,r.update=a,r.asReadonly=$f.bind(r),r}function $f(){let t=this[Ke];if(t.readonlyFn===void 0){let n=()=>this();n[Ke]=t,t.readonlyFn=n}return t.readonlyFn}var Bi=new y("",{factory:()=>dw}),dw="ng";var Is=new y(""),ji=new y("",{providedIn:"platform",factory:()=>"unknown"}),xr=new y(""),zi=new y("",{factory:()=>m(ee).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var Da=(()=>{class t{view;node;constructor(e,i){this.view=e,this.node=i}static __NG_ELEMENT_ID__=mw}return t})();function mw(){return new Da(U(),Xe())}var Rn=class{},wr=new y("",{factory:()=>!0});var Kd=new y(""),Ns=(()=>{class t{static \u0275prov=Te({token:t,providedIn:"root",factory:()=>new cd})}return t})(),cd=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let e=n.zone,i=this.queues.get(e);i.has(n)&&(i.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let e=n.zone;this.queues.has(e)||this.queues.set(e,new Set);let i=this.queues.get(e);i.has(n)||i.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[e,i]of this.queues)e===null?n||=this.flushQueue(i):n||=e.run(()=>this.flushQueue(i));n||(this.dirtyEffectCount=0)}}flushQueue(n){let e=!1;for(let i of n)i.dirty&&(this.dirtyEffectCount--,e=!0,i.run());return e}},as=class{[Ke];constructor(n){this[Ke]=n}destroy(){this[Ke].destroy()}};function Jt(t,n){let e=n?.injector??m(X),i=n?.manualCleanup!==!0?e.get(Et):null,a,r=e.get(Da,null,{optional:!0}),o=e.get(Rn);return r!==null?(a=Wf(r.view,o,t),i instanceof ns&&i._lView===r.view&&(i=null)):a=hw(t,e.get(Ns),o),a.injector=e,i!==null&&(a.onDestroyFns=[i.onDestroy(()=>a.destroy())]),new as(a)}var Gf=ge(P({},Ol),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let t=lr(!1);try{Fl(this)}finally{lr(t)}},cleanup(){if(!this.cleanupFns?.length)return;let t=j(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],j(t)}}}),uw=ge(P({},Gf),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(Zn(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.scheduler.remove(this)}}),pw=ge(P({},Gf),{consumerMarkedDirty(){this.view[Z]|=8192,Li(this.view),this.notifier.notify(13)},destroy(){if(Zn(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.view[Tn]?.delete(this)}});function Wf(t,n,e){let i=Object.create(pw);return i.view=t,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=n,i.userFn=e,i.fn=qf(i,e),t[Tn]??=new Set,t[Tn].add(i),i.consumerMarkedDirty(i),i}function hw(t,n,e){let i=Object.create(uw);return i.userFn=t,i.fn=qf(i,t),i.scheduler=n,i.notifier=e,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function qf(t,n){return()=>{n(e=>(t.cleanupFns??=[]).push(e))}}function Lt(t){return typeof t=="function"&&t[Ke]!==void 0}function Ms(t){return Lt(t)&&typeof t.set=="function"}var Xs=Symbol("InputSignalNode#UNSET"),Mg=ge(P({},oa),{transformFn:void 0,applyValueToInputSignal(t,n){gi(t,n)}});function Ar(t){return{toString:t}.toString()}var be=(function(t){return t[t.TemplateCreateStart=0]="TemplateCreateStart",t[t.TemplateCreateEnd=1]="TemplateCreateEnd",t[t.TemplateUpdateStart=2]="TemplateUpdateStart",t[t.TemplateUpdateEnd=3]="TemplateUpdateEnd",t[t.LifecycleHookStart=4]="LifecycleHookStart",t[t.LifecycleHookEnd=5]="LifecycleHookEnd",t[t.OutputStart=6]="OutputStart",t[t.OutputEnd=7]="OutputEnd",t[t.BootstrapApplicationStart=8]="BootstrapApplicationStart",t[t.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",t[t.BootstrapComponentStart=10]="BootstrapComponentStart",t[t.BootstrapComponentEnd=11]="BootstrapComponentEnd",t[t.ChangeDetectionStart=12]="ChangeDetectionStart",t[t.ChangeDetectionEnd=13]="ChangeDetectionEnd",t[t.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",t[t.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",t[t.AfterRenderHooksStart=16]="AfterRenderHooksStart",t[t.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",t[t.ComponentStart=18]="ComponentStart",t[t.ComponentEnd=19]="ComponentEnd",t[t.DeferBlockStateStart=20]="DeferBlockStateStart",t[t.DeferBlockStateEnd=21]="DeferBlockStateEnd",t[t.DynamicComponentStart=22]="DynamicComponentStart",t[t.DynamicComponentEnd=23]="DynamicComponentEnd",t[t.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",t[t.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",t})(be||{}),Vs=class{previousValue;currentValue;firstChange;constructor(n,e,i){this.previousValue=n,this.currentValue=e,this.firstChange=i}isFirstChange(){return this.firstChange}};function Tg(t,n,e,i){n!==null?n.applyValueToInputSignal(n,i):t[e]=i}var Ag=null,rt=(()=>{Ag=Yf;let t=()=>Yf;return t.ngInherit=!0,t})();function Iw(){return Ag}function Yf(t){return t.type.prototype.ngOnChanges&&(t.setInput=Mw),Nw}function Nw(){let t=Rg(this),n=t?.current;if(n){let e=t.previous;if(e===Xn)t.previous=n;else for(let i in n)e[i]=n[i];t.current=null,this.ngOnChanges(n)}}function Mw(t,n,e,i,a){let r=this.declaredInputs[i],o=Rg(t)||Tw(t,{previous:Xn,current:null}),s=o.current||(o.current={}),c=o.previous,l=c[r];s[r]=new Vs(l&&l.currentValue,e,c===Xn),Tg(t,n,a,e)}var sm="__ngSimpleChanges__";function Rg(t){return Object.hasOwn(t,sm)&&t[sm]||null}function Tw(t,n){return t[sm]=n}var Zf=[];var ke=function(t,n=null,e){for(let i=0;i<Zf.length;i++){let a=Zf[i];a(t,n,e)}};function Aw(t,n,e){let{ngOnChanges:i,ngOnInit:a,ngDoCheck:r}=n.type.prototype;if(i){let o=Iw()(n);(e.preOrderHooks??=[]).push(t,o),(e.preOrderCheckHooks??=[]).push(t,o)}a&&(e.preOrderHooks??=[]).push(0-t,a),r&&((e.preOrderHooks??=[]).push(t,r),(e.preOrderCheckHooks??=[]).push(t,r))}function Og(t,n){for(let e=n.directiveStart,i=n.directiveEnd;e<i;e++){let r=t.data[e].type.prototype,{ngAfterContentInit:o,ngAfterContentChecked:s,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:d}=r;o&&(t.contentHooks??=[]).push(-e,o),s&&((t.contentHooks??=[]).push(e,s),(t.contentCheckHooks??=[]).push(e,s)),c&&(t.viewHooks??=[]).push(-e,c),l&&((t.viewHooks??=[]).push(e,l),(t.viewCheckHooks??=[]).push(e,l)),d!=null&&(t.destroyHooks??=[]).push(e,d)}}function Os(t,n,e){Fg(t,n,3,e)}function Fs(t,n,e,i){(t[Z]&3)===e&&Fg(t,n,e,i)}function Qd(t,n){let e=t[Z];(e&3)===n&&(e&=16383,e+=1,t[Z]=e)}function Fg(t,n,e,i){let a=i!==void 0?t[Ti]&65535:0,r=i??-1,o=n.length-1,s=0;for(let c=a;c<o;c++)if(typeof n[c+1]=="number"){if(s=n[c],i!=null&&s>=i)break}else n[c]<0&&(t[Ti]+=65536),(s<r||r==-1)&&(Rw(t,e,n,c),t[Ti]=(t[Ti]&4294901760)+c+2),c++}function Kf(t,n){ke(be.LifecycleHookStart,t,n);let e=j(null);try{n.call(t)}finally{j(e),ke(be.LifecycleHookEnd,t,n)}}function Rw(t,n,e,i){let a=e[i]<0,r=e[i+1],o=a?-e[i]:e[i],s=t[o];a?t[Z]>>14<t[Ti]>>16&&(t[Z]&3)===n&&(t[Z]+=16384,Kf(s,r)):Kf(s,r)}var Ma=-1,Ui=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,e,i,a){this.factory=n,this.name=a,this.canSeeViewProviders=e,this.injectImpl=i}};function Ow(t){return(t.flags&8)!==0}function Fw(t){return(t.flags&16)!==0}function Lw(t,n,e){let i=0;for(;i<e.length;){let a=e[i];if(typeof a=="number"){if(a!==0)break;i++;let r=e[i++],o=e[i++],s=e[i++];t.setAttribute(n,o,s,r)}else{let r=a,o=e[++i];Pw(r)?t.setProperty(n,r,o):t.setAttribute(n,r,o),i++}}return i}function Lg(t){return t===3||t===4||t===6}function Pw(t){return t.charCodeAt(0)===64}function Ta(t,n){if(!(n===null||n.length===0))if(t===null||t.length===0)t=n.slice();else{let e=-1;for(let i=0;i<n.length;i++){let a=n[i];typeof a=="number"?e=a:e===0||(e===-1||e===2?Qf(t,e,a,null,n[++i]):Qf(t,e,a,null,null))}}return t}function Qf(t,n,e,i,a){let r=0,o=t.length;if(n===-1)o=-1;else for(;r<t.length;){let s=t[r++];if(typeof s=="number"){if(s===n){o=-1;break}else if(s>n){o=r-1;break}}}for(;r<t.length;){let s=t[r];if(typeof s=="number")break;if(s===e){a!==null&&(t[r+1]=a);return}r++,a!==null&&r++}o!==-1&&(t.splice(o,0,n),r=o+1),t.splice(r++,0,e),a!==null&&t.splice(r++,0,a)}function Pg(t){return t!==Ma}function Bs(t){return t&32767}function Vw(t){return t>>16}function js(t,n){let e=Vw(t),i=n;for(;e>0;)i=i[Jn],e--;return i}var cm=!0;function Xf(t){let n=cm;return cm=t,n}var Bw=256,Vg=Bw-1,Bg=5,jw=0,fn={};function zw(t,n,e){let i;typeof e=="string"?i=e.charCodeAt(0)||0:Object.hasOwn(e,Di)&&(i=e[Di]),i==null&&(i=e[Di]=jw++);let a=i&Vg,r=1<<a;n.data[t+(a>>Bg)]|=r}function zs(t,n){let e=jg(t,n);if(e!==-1)return e;let i=n[W];i.firstCreatePass&&(t.injectorIndex=n.length,Xd(i.data,t),Xd(n,null),Xd(i.blueprint,null));let a=Um(t,n),r=t.injectorIndex;if(Pg(a)){let o=Bs(a),s=js(a,n),c=s[W].data;for(let l=0;l<8;l++)n[r+l]=s[o+l]|c[o+l]}return n[r+8]=a,r}function Xd(t,n){t.push(0,0,0,0,0,0,0,0,n)}function jg(t,n){return t.injectorIndex===-1||t.parent&&t.parent.injectorIndex===t.injectorIndex||n[t.injectorIndex+8]===null?-1:t.injectorIndex}function Um(t,n){if(t.parent&&t.parent.injectorIndex!==-1)return t.parent.injectorIndex;let e=0,i=null,a=n;for(;a!==null;){if(i=Gg(a),i===null)return Ma;if(e++,a=a[Jn],i.injectorIndex!==-1)return i.injectorIndex|e<<16}return Ma}function lm(t,n,e){zw(t,n,e)}function Hw(t,n){if(n==="class")return t.classes;if(n==="style")return t.styles;let e=t.attrs;if(e){let i=e.length,a=0;for(;a<i;){let r=e[a];if(Lg(r))break;if(r===0)a=a+2;else if(typeof r=="number")for(a++;a<i&&typeof e[a]=="string";)a++;else{if(r===n)return e[a+1];a=a+2}}}return null}function zg(t,n,e){if(e&8||t!==void 0)return t;ds(n,"NodeInjector")}function Hg(t,n,e,i){if(e&8&&i===void 0&&(i=null),(e&3)===0){let a=t[On],r=Tt(void 0);try{return a?a.get(n,i,e&8):fd(n,i,e&8)}finally{Tt(r)}}return zg(i,n,e)}function Ug(t,n,e,i=0,a){if(t!==null){if(n[Z]&2048&&!(i&2)){let o=Ww(t,n,e,i,fn);if(o!==fn)return o}let r=$g(t,n,e,i,fn);if(r!==fn)return r}return Hg(n,e,i,a)}function $g(t,n,e,i,a){let r=$w(e);if(typeof r=="function"){if(!Ud(n,t,i))return i&1?zg(a,e,i):Hg(n,e,i,a);try{let o;if(o=r(i),o==null&&!(i&8))ds(e);else return o}finally{$d()}}else if(typeof r=="number"){let o=null,s=jg(t,n),c=Ma,l=i&1?n[gt][lt]:null;for((s===-1||i&4)&&(c=s===-1?Um(t,n):n[s+8],c===Ma||!eg(i,!1)?s=-1:(o=n[W],s=Bs(c),n=js(c,n)));s!==-1;){let d=n[W];if(Jf(r,s,d.data)){let u=Uw(s,n,e,o,i,l);if(u!==fn)return u}c=n[s+8],c!==Ma&&eg(i,n[W].data[s+8]===l)&&Jf(r,s,n)?(o=d,s=Bs(c),n=js(c,n)):s=-1}}return a}function Uw(t,n,e,i,a,r){let o=n[W],s=o.data[t+8],c=i==null?pn(s)&&cm:i!=o&&(s.type&3)!==0,l=a&1&&r===s,d=Ls(s,o,e,c,l);return d!==null?Er(n,o,d,s,a):fn}function Ls(t,n,e,i,a){let r=t.providerIndexes,o=n.data,s=r&1048575,c=t.directiveStart,l=t.directiveEnd,d=r>>20,u=i?s:s+d,g=a?s+d:l;for(let f=u;f<g;f++){let _=o[f];if(f<c&&e===_||f>=c&&_.type===e)return f}if(a){let f=o[c];if(f&&hn(f)&&f.type===e)return c}return null}function Er(t,n,e,i,a){let r=t[e],o=n.data;if(r instanceof Ui){let s=r;if(s.resolving)throw hd("");let c=Xf(s.canSeeViewProviders);s.resolving=!0;let l=o[e].type||o[e],d,u=s.injectImpl?Tt(s.injectImpl):null,g=Ud(t,i,0);try{r=t[e]=s.factory(void 0,a,o,t,i),n.firstCreatePass&&e>=i.directiveStart&&Aw(e,o[e],n)}finally{u!==null&&Tt(u),Xf(c),s.resolving=!1,$d()}}return r}function $w(t){if(typeof t=="string")return t.charCodeAt(0)||0;let n=Object.hasOwn(t,Di)?t[Di]:void 0;return typeof n=="number"?n>=0?n&Vg:Gw:n}function Jf(t,n,e){let i=1<<t;return!!(e[n+(t>>Bg)]&i)}function eg(t,n){return!(t&2)&&!(t&1&&n)}var Pn=class{_tNode;_lView;constructor(n,e){this._tNode=n,this._lView=e}get(n,e,i){return Ug(this._tNode,this._lView,n,ki(i),e)}};function Gw(){return new Pn(Xe(),U())}function Fe(t){return Ar(()=>{let n=t.prototype.constructor,e=n[cr]||dm(n),i=Object.prototype,a=Object.getPrototypeOf(t.prototype).constructor;for(;a&&a!==i;){let r=a[cr]||dm(a);if(r&&r!==e)return r;a=Object.getPrototypeOf(a)}return r=>new r})}function dm(t){return dd(t)?()=>{let n=dm(st(t));return n&&n()}:Ci(t)}function Ww(t,n,e,i,a){let r=t,o=n;for(;r!==null&&o!==null&&o[Z]&2048&&!Sa(o);){let s=$g(r,o,e,i|2,fn);if(s!==fn)return s;i&=-5;let c=r.parent;if(!c){let l=o[kd];if(l){let d=l.get(e,fn,i);if(d!==fn)return d}c=Gg(o),o=o[Jn]}r=c}return a}function Gg(t){let n=t[W],e=n.type;return e===2?n.declTNode:e===1?t[lt]:null}function $m(t){return Hw(Xe(),t)}function Y(t){return{token:t.token,providedIn:t.autoProvided===!1?null:"root",factory:t.factory,value:void 0}}function qw(){return Pa(Xe(),U())}function Pa(t,n){return new L(Ot(t,n))}var L=(()=>{class t{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=qw}return t})();function Wg(t){return t instanceof L?t.nativeElement:t}function Yw(){return this._results[Symbol.iterator]()}var nn=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new E}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,e){return this._results.reduce(n,e)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,e){this.dirty=!1;let i=of(n);(this._changesDetected=!rf(this._results,i,e))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=Yw};function qg(t){return(t.flags&128)===128}var Gm=(function(t){return t[t.OnPush=0]="OnPush",t[t.Eager=1]="Eager",t[t.Default=1]="Default",t})(Gm||{}),Yg=new Map,Zw=0;function Kw(){return Zw++}function Qw(t){Yg.set(t[gr],t)}function mm(t){Yg.delete(t[gr])}var tg="__ngContext__";function Aa(t,n){Fn(n)?(t[tg]=n[gr],Qw(n)):t[tg]=n}function Zg(t){return Qg(t[wa])}function Kg(t){return Qg(t[zt])}function Qg(t){for(;t!==null&&!Ht(t);)t=t[zt];return t}var um;function Wm(t){um=t}function Xg(){if(um!==void 0)return um;if(typeof document<"u")return document;throw new H(210,!1)}var Jg="r";var eb="di";var tb=!1,nb=new y("",{factory:()=>tb});var ng=new WeakMap;function Xw(t,n){if(t==null||typeof t!="object")return;let e=ng.get(t);e||(e=new WeakSet,ng.set(t,e)),e.add(n)}var Jw=(t,n,e,i)=>{};function ek(t,n,e,i){Jw(t,n,e,i)}function Js(t){return(t.flags&32)===32}var tk=()=>null;function ib(t,n,e=!1){return tk(t,n,e)}function ab(t,n){let e=t.contentQueries;if(e!==null){let i=j(null);try{for(let a=0;a<e.length;a+=2){let r=e[a],o=e[a+1];if(o!==-1){let s=t.data[o];_r(r),s.contentQueries(2,n[o],o)}}}finally{j(i)}}}function pm(t,n,e){_r(0);let i=j(null);try{n(t,e)}finally{j(i)}}function qm(t,n,e){if(Sd(n)){let i=j(null);try{let a=n.directiveStart,r=n.directiveEnd;for(let o=a;o<r;o++){let s=t.data[o];if(s.contentQueries){let c=e[o];s.contentQueries(1,c,o)}}}finally{j(i)}}}var an=(function(t){return t[t.Emulated=0]="Emulated",t[t.None=2]="None",t[t.ShadowDom=3]="ShadowDom",t[t.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",t})(an||{}),Vn=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity}`}},hm=class extends Vn{getTypeName(){return"HTML"}},fm=class extends Vn{getTypeName(){return"Style"}},gm=class extends Vn{getTypeName(){return"Script"}},bm=class extends Vn{getTypeName(){return"URL"}},vm=class extends Vn{getTypeName(){return"ResourceURL"}};function $t(t){return t instanceof Vn?t.changingThisBreaksApplicationSecurity:t}function ri(t,n){let e=rb(t);if(e!=null&&e!==n){if(e==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${e} (see ${ld})`)}return e===n}function rb(t){return t instanceof Vn&&t.getTypeName()||null}function Ym(t){return new hm(t)}function Zm(t){return new fm(t)}function Km(t){return new gm(t)}function Qm(t){return new bm(t)}function Xm(t){return new vm(t)}var Ts;function nk(){if(Ts===void 0&&(Ts=null,va.trustedTypes))try{Ts=va.trustedTypes.createPolicy("angular",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return Ts}function ec(t){return nk()?.createHTML(t)||t}function ik(t){let n=new ym(t);return ak()?new _m(n):n}var _m=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let e=new window.DOMParser().parseFromString(ec(n),"text/html").body;return e===null?this.inertDocumentHelper.getInertBodyElement(n):(e.firstChild?.remove(),e)}catch{return null}}},ym=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let e=this.inertDocument.createElement("template");return e.innerHTML=ec(n),e}};function ak(){try{return!!new window.DOMParser().parseFromString(ec(""),"text/html")}catch{return!1}}var rk=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function Rr(t){return t=String(t),t.match(rk)?t:"unsafe:"+t}function Bn(t){let n=Object.create(null);for(let e of t.split(","))n[e]=!0;return n}function Or(...t){let n=Object.create(null);for(let e of t)for(let i in e)Object.hasOwn(e,i)&&(n[i]=!0);return n}var ob=Bn("area,br,col,hr,img,wbr"),sb=Bn("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),cb=Bn("rp,rt"),ok=Or(cb,sb),sk=Or(sb,Bn("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),ck=Or(cb,Bn("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),ig=Or(ob,sk,ck,ok),lb=Bn("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),lk=Bn("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),dk=Bn("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),mk=Or(lb,lk,dk),uk=Bn("script,style,template"),xm=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let e=n.firstChild,i=!0,a=[];for(;e;){if(e.nodeType===Node.ELEMENT_NODE?i=this.startElement(e):e.nodeType===Node.TEXT_NODE?this.chars(e.nodeValue):this.sanitizedSomething=!0,i&&e.firstChild){a.push(e),e=fk(e);continue}for(;e;){e.nodeType===Node.ELEMENT_NODE&&this.endElement(e);let r=hk(e);if(r){e=r;break}e=a.pop()}}return this.buf.join("")}startElement(n){let e=ag(n).toLowerCase();if(!Object.hasOwn(ig,e))return this.sanitizedSomething=!0,!Object.hasOwn(uk,e);this.buf.push("<"),this.buf.push(e);let i=n.attributes;for(let a=0;a<i.length;a++){let r=i.item(a),o=r.name,s=o.toLowerCase();if(!Object.hasOwn(mk,s)){this.sanitizedSomething=!0;continue}let c=r.value;lb[s]&&(c=Rr(c)),this.buf.push(" ",o,'="',rg(c),'"')}return this.buf.push(">"),!0}endElement(n){let e=ag(n).toLowerCase();Object.hasOwn(ig,e)&&!Object.hasOwn(ob,e)&&(this.buf.push("</"),this.buf.push(e),this.buf.push(">"))}chars(n){this.buf.push(rg(n))}};function pk(t,n){return(t.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function hk(t){let n=t.nextSibling;if(n&&t!==n.previousSibling)throw db(n);return n}function fk(t){let n=t.firstChild;if(n&&pk(t,n))throw db(n);return n}function ag(t){let n=t.nodeName;return typeof n=="string"?n:"FORM"}function db(t){return new Error(`Failed to sanitize html because the element is clobbered: ${t.outerHTML}`)}var gk=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,bk=/([^\#-~ |!])/g;function rg(t){return t.replace(/&/g,"&amp;").replace(gk,function(n){let e=n.charCodeAt(0),i=n.charCodeAt(1);return"&#"+((e-55296)*1024+(i-56320)+65536)+";"}).replace(bk,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var As;function Jm(t,n){let e=null;try{As=As||ik(t);let i=n?String(n):"";e=As.getInertBodyElement(i);let a=5,r=i;do{if(a===0)throw new Error("Failed to sanitize html because the input is unstable");a--,i=r,r=e.innerHTML,e=As.getInertBodyElement(i)}while(i!==r);let s=new xm().sanitizeChildren(og(e)||e);return ec(s)}finally{if(e){let i=og(e)||e;for(;i.firstChild;)i.firstChild.remove()}}}function og(t){return"content"in t&&vk(t)?t.content:null}function vk(t){return t.nodeType===Node.ELEMENT_NODE&&t.nodeName==="TEMPLATE"}var _k=/(^-?|--!?)>|<(?=!--|!-$)/g,yk=/(<|>)/g,xk="\u200B$1\u200B";function wk(t){return t.replace(_k,n=>n.replace(yk,xk))}function kk(t,n){return t.createText(n)}function Ck(t,n,e){t.setValue(n,e)}function Sk(t,n){return t.createComment(wk(n))}function mb(t,n,e){return t.createElement(n,e)}function Hi(t,n,e,i,a){t.insertBefore(n,e,i,a)}function ub(t,n,e){t.appendChild(n,e)}function sg(t,n,e,i,a){i!==null?Hi(t,n,e,i,a):ub(t,n,e)}function pb(t,n,e,i){t.removeChild(null,n,e,i)}function Ek(t,n,e){t.setAttribute(n,"style",e)}function Dk(t,n,e){e===""?t.removeAttribute(n,"class"):t.setAttribute(n,"class",e)}function hb(t,n,e){let{mergedAttrs:i,classes:a,styles:r}=e;i!==null&&Lw(t,n,i),a!==null&&Dk(t,n,a),r!==null&&Ek(t,n,r)}function eu(t){let n=Ik();return n?n.sanitize(Rt.URL,t)||"":ri(t,"URL")?$t(t):Rr(ba(t))}function Ik(){let t=U();return t&&t[Qt].sanitizer}function Nk(t,n,e){let i=t.length;for(;;){let a=t.indexOf(n,e);if(a===-1)return a;if(a===0||t.charCodeAt(a-1)<=32){let r=n.length;if(a+r===i||t.charCodeAt(a+r)<=32)return a}e=a+1}}var fb="ng-template";function Mk(t,n,e,i){let a=0;if(i){for(;a<n.length&&typeof n[a]=="string";a+=2)if(n[a]==="class"&&Nk(n[a+1].toLowerCase(),e,0)!==-1)return!0}else if(tu(t))return!1;if(a=n.indexOf(1,a),a>-1){let r;for(;++a<n.length&&typeof(r=n[a])=="string";)if(r.toLowerCase()===e)return!0}return!1}function tu(t){return t.type===4&&t.value!==fb}function Tk(t,n,e){let i=t.type===4&&!e?fb:t.value;return n===i}function Ak(t,n,e){let i=4,a=t.attrs,r=a!==null?Fk(a):0,o=!1;for(let s=0;s<n.length;s++){let c=n[s];if(typeof c=="number"){if(!o&&!en(i)&&!en(c))return!1;if(o&&en(c))continue;o=!1,i=c|i&1;continue}if(!o)if(i&4){if(i=2|i&1,c!==""&&!Tk(t,c,e)||c===""&&n.length===1){if(en(i))return!1;o=!0}}else if(i&8){if(a===null||!Mk(t,a,c,e)){if(en(i))return!1;o=!0}}else{let l=n[++s],d=Rk(c,a,tu(t),e);if(d===-1){if(en(i))return!1;o=!0;continue}if(l!==""){let u;if(d>r?u="":u=a[d+1].toLowerCase(),i&2&&l!==u){if(en(i))return!1;o=!0}}}}return en(i)||o}function en(t){return(t&1)===0}function Rk(t,n,e,i){if(n===null)return-1;let a=0;if(i||!e){let r=!1;for(;a<n.length;){let o=n[a];if(o===t)return a;if(o===3||o===6)r=!0;else if(o===1||o===2){let s=n[++a];for(;typeof s=="string";)s=n[++a];continue}else{if(o===4)break;if(o===0){a+=4;continue}}a+=r?1:2}return-1}else return Lk(n,t)}function gb(t,n,e=!1){for(let i=0;i<n.length;i++)if(Ak(t,n[i],e))return!0;return!1}function Ok(t){let n=t.attrs;if(n!=null){let e=n.indexOf(5);if((e&1)===0)return n[e+1]}return null}function Fk(t){for(let n=0;n<t.length;n++){let e=t[n];if(Lg(e))return n}return t.length}function Lk(t,n){let e=t.indexOf(4);if(e>-1)for(e++;e<t.length;){let i=t[e];if(typeof i=="number")return-1;if(i===n)return e;e++}return-1}function Pk(t,n){e:for(let e=0;e<n.length;e++){let i=n[e];if(t.length===i.length){for(let a=0;a<t.length;a++)if(t[a]!==i[a])continue e;return!0}}return!1}function cg(t,n){return t?":not("+n.trim()+")":n}function Vk(t){let n=t[0],e=1,i=2,a="",r=!1;for(;e<t.length;){let o=t[e];if(typeof o=="string")if(i&2){let s=t[++e];a+="["+o+(s.length>0?'="'+s+'"':"")+"]"}else i&8?a+="."+o:i&4&&(a+=" "+o);else a!==""&&!en(o)&&(n+=cg(r,a),a=""),i=o,r=r||!en(i);e++}return a!==""&&(n+=cg(r,a)),n}function Bk(t){return t.map(Vk).join(",")}function jk(t){let n=[],e=[],i=1,a=2;for(;i<t.length;){let r=t[i];if(typeof r=="string")a===2?r!==""&&n.push(r,t[++i]):a===8&&e.push(r);else{if(!en(a))break;a=r}i++}return e.length&&n.push(1,...e),n}var Dt={},gn=(function(t){return t[t.Important=1]="Important",t[t.DashCase=2]="DashCase",t})(gn||{}),zk;function nu(t,n){return zk(t,n)}var iu=new Set;var JL=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var wm=new WeakMap;function bb(t){return t?t[Jn]??t:null}var Ia=new WeakSet;function Hk(t,n,e){let i=wm.get(t);if(!i||i.length===0)return;let a=n.parentNode,r=n.previousSibling,o=bb(e);for(let s=i.length-1;s>=0;s--){let{el:c,declarationView:l}=i[s],d=c.parentNode;c===n?(i.splice(s,1),Ia.add(c),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):r&&c===r?(i.splice(s,1),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),c.parentNode?.removeChild(c)):d&&a&&d!==a&&(o===null||l===null||o===l)&&(i.splice(s,1),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),c.parentNode?.removeChild(c))}}function Uk(t,n,e){let i=bb(e),a=wm.get(t);a?a.some(r=>r.el===n)||a.push({el:n,declarationView:i}):wm.set(t,[{el:n,declarationView:i}])}var tc=(function(t){return t[t.CHANGE_DETECTION=0]="CHANGE_DETECTION",t[t.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",t})(tc||{}),bn=new y(""),lg=new Set;function jn(t){lg.has(t)||(lg.add(t),performance?.mark?.("mark_feature_usage",{detail:{feature:t}}))}var nc=(()=>{class t{impl=null;execute(){this.impl?.execute()}static \u0275prov=Te({token:t,providedIn:"root",factory:()=>new t})}return t})(),au=[0,1,2,3],ru=(()=>{class t{ngZone=m(N);scheduler=m(Rn);errorHandler=m(mn,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){m(bn,{optional:!0})}execute(){let e=this.sequences.size>0;e&&ke(be.AfterRenderHooksStart),this.executing=!0;for(let i of au)for(let a of this.sequences)if(!(a.erroredOrDestroyed||!a.hooks[i]))try{a.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let r=a.hooks[i];return r(a.pipelinedValue)},a.snapshot))}catch(r){a.erroredOrDestroyed=!0,this.errorHandler?.handleError(r)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&ke(be.AfterRenderHooksEnd)}register(e){let{view:i}=e;i!==void 0?((i[Ai]??=[]).push(e),Li(i),i[Z]|=8192):this.addSequence(e)}addSequence(e){this.executing?this.deferredRegistrations.add(e):this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,i){return i?i.run(tc.AFTER_NEXT_RENDER,e):e()}static \u0275prov=Te({token:t,providedIn:"root",factory:()=>new t})}return t})(),Dr=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,e,i,a,r,o=null){this.impl=n,this.hooks=e,this.view=i,this.once=a,this.snapshot=o,this.unregisterOnDestroy=r?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[Ai];n&&(this.view[Ai]=n.filter(e=>e!==this))}};function mt(t,n){let e=n?.injector??m(X);return jn("NgAfterNextRender"),Gk(t,e,n,!0)}function $k(t){return t instanceof Function?[void 0,void 0,t,void 0]:[t.earlyRead,t.write,t.mixedReadWrite,t.read]}function Gk(t,n,e,i){let a=n.get(nc);a.impl??=n.get(ru);let r=n.get(bn,null,{optional:!0}),o=e?.manualCleanup!==!0?n.get(Et):null,s=n.get(Da,null,{optional:!0}),c=new Dr(a.impl,$k(t),s?.view,i,o,r?.snapshot(null));return a.impl.register(c),c}var km=null,vb=null;function dg(t,n,e,i){km?.maybeQueueEnterAnimation(t,n,e,i)}function mg(t,n,e,i){km===null?i(!1):km.runLeaveAnimationsWithCallback(t,n,e,i)}function Wk(t){vb?.initViewDetachAnimations(t)}function qk(t){vb?.clearViewDetachAnimations(t)}function Na(t,n,e,i,a,r,o,s){if(a!=null){let c,l=!1;Ht(a)?c=a:Fn(a)&&(l=!0,a=a[Kt]);let d=dt(a);t===0&&i!==null?(dg(s,i,r,e),o==null?ub(n,i,d):Hi(n,i,d,o||null,!0)):t===1&&i!==null?(Hi(n,i,d,o||null,!0),Hk(r,d,s),Ia.has(d)||dg(s,i,r,e)):t===2?(s?.[ff]?.leave?.has(r.index)&&Uk(r,d,s),Ia.delete(d),mg(s,r,e,u=>{if(Ia.has(d)){Ia.delete(d);return}pb(n,d,l,u)})):t===3&&(Ia.delete(d),mg(s,r,e,()=>{n.destroyNode(d)})),c!=null&&aC(n,t,e,c,r,i,o)}}function Yk(t,n){_b(t,n),n[Kt]=null,n[lt]=null}function Zk(t,n,e,i,a,r){i[Kt]=a,i[lt]=n,ac(t,i,e,1,a,r)}function _b(t,n){n[Qt].changeDetectionScheduler?.notify(9),ac(t,n,n[De],2,null,null)}function Kk(t){let n=t[wa];if(!n)return Jd(t[W],t);for(;n;){let e=null;if(Fn(n))e=n[wa];else{let i=n[Ue];i&&(e=i)}if(!e){for(;n&&!n[zt]&&n!==t;)Fn(n)&&Jd(n[W],n),n=n[$e];n===null&&(n=t),Fn(n)&&Jd(n[W],n),e=n&&n[zt]}n=e}}function ou(t,n){let e=t[Oi],i=e.indexOf(n);e.splice(i,1)}function ic(t,n){if(Fi(n))return;let e=n[De];e.destroyNode&&ac(t,n,e,3,null,null),Kk(n)}function Jd(t,n){if(Fi(n))return;let e=j(null);try{n[Z]&=-129,n[Z]|=256,n[At]&&Zn(n[At]),Xk(t,n),Qk(t,n),n[W].type===1&&n[De].destroy();let i=n[ei];if(i!==null&&Ht(n[$e])){i!==n[$e]&&ou(i,n);let a=n[un];a!==null&&a.detachView(t)}mm(n)}finally{j(e)}}function Qk(t,n){let e=t.cleanup,i=n[xa];if(e!==null)for(let o=0;o<e.length-1;o+=2)if(typeof e[o]=="string"){let s=e[o+3];s>=0?i[s]():i[-s].unsubscribe(),o+=2}else{let s=i[e[o+1]];e[o].call(s)}i!==null&&(n[xa]=null);let a=n[Mn];if(a!==null){n[Mn]=null;for(let o=0;o<a.length;o++){let s=a[o];s()}}let r=n[Tn];if(r!==null){n[Tn]=null;for(let o of r)o.destroy()}}function Xk(t,n){let e;if(t!=null&&(e=t.destroyHooks)!=null)for(let i=0;i<e.length;i+=2){let a=n[e[i]];if(!(a instanceof Ui)){let r=e[i+1];if(Array.isArray(r))for(let o=0;o<r.length;o+=2){let s=a[r[o]],c=r[o+1];ke(be.LifecycleHookStart,s,c);try{c.call(s)}finally{ke(be.LifecycleHookEnd,s,c)}}else{ke(be.LifecycleHookStart,a,r);try{r.call(a)}finally{ke(be.LifecycleHookEnd,a,r)}}}}}function yb(t,n,e){if(n===null)throw new H(510,!1);return Jk(t,n.parent,e)}function Jk(t,n,e){let i=n;for(;i!==null&&i.type&168;)n=i,i=n.parent;if(i===null)return e[Kt];if(pn(i)){let{encapsulation:a}=t.data[i.directiveStart+i.componentOffset];if(a===an.None||a===an.Emulated)return null}return Ot(i,e)}function xb(t,n,e){return tC(t,n,e)}function eC(t,n,e){return t.type&40?Ot(t,e):null}var tC=eC,ug;function su(t,n,e,i){let a=yb(t,i,n),r=n[De],o=i.parent||n[lt],s=xb(o,i,n);if(a!=null)if(Array.isArray(e))for(let c=0;c<e.length;c++)sg(r,a,e[c],s,!1);else sg(r,a,e,s,!1);ug!==void 0&&ug(r,i,n,e,a)}function kr(t,n){if(n!==null){let e=n.type;if(e&3)return Ot(n,t);if(e&4)return Cm(-1,t[n.index]);if(e&8){let i=n.child;if(i!==null)return kr(t,i);{let a=t[n.index];return Ht(a)?Cm(-1,a):dt(a)}}else{if(e&128)return kr(t,n.next);if(e&32)return nu(n,t)()||dt(t[n.index]);{let i=wb(t,n);if(i!==null){if(Array.isArray(i))return i[0];let a=An(t[gt]);return kr(a,i)}else return kr(t,n.next)}}}return null}function wb(t,n){if(n!==null){let i=t[gt][lt],a=n.projection;return i.projection[a]}return null}function Cm(t,n){let e=Ue+t+1;if(e<n.length){let i=n[e],a=i[W].firstChild;if(a!==null)return kr(i,a)}return n[Ri]}function cu(t,n,e,i,a,r,o){for(;e!=null;){let s=i[On];if(e.type===128){e=e.next;continue}let c=i[e.index],l=e.type;if(o&&n===0&&(c&&Aa(dt(c),i),e.flags|=2),!Js(e))if(l&8)cu(t,n,e.child,i,a,r,!1),Na(n,t,s,a,c,e,r,i);else if(l&32){let d=nu(e,i),u;for(;u=d();)Na(n,t,s,a,u,e,r,i);Na(n,t,s,a,c,e,r,i)}else l&16?kb(t,n,i,e,a,r):Na(n,t,s,a,c,e,r,i);e=o?e.projectionNext:e.next}}function ac(t,n,e,i,a,r){t.type===3?nC(e,i,n,a,r):cu(e,i,t.firstChild,n,a,r,!1)}function nC(t,n,e,i,a){let o=e[W].firstChild,s=o.next,c=dt(e[o.index]),l=dt(e[s.index]),d=s.index+1,u=e[d];if(n===1||n===0)i!==null&&(u&&u.hasChildNodes()?Hi(t,i,u,a,!0):(Hi(t,i,c,a,!0),Hi(t,i,l,a,!0)));else if(n===2){if(u||(u=document.createDocumentFragment(),e[d]=u),c&&c.parentNode===u)return;let g=c;for(;g!==null;){let f=g.nextSibling;if(u.appendChild(g),g===l)break;g=f}}}function iC(t,n,e){let i=n[De],a=yb(t,e,n),r=e.parent||n[lt],o=xb(r,e,n);kb(i,0,n,e,a,o)}function kb(t,n,e,i,a,r){let o=e[gt],c=o[lt].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let d=c[l];Na(n,t,e[On],a,d,i,r,e)}else{let l=c,d=o[$e];qg(i)&&(l.flags|=128),cu(t,n,l,d,a,r,!0)}}function aC(t,n,e,i,a,r,o){let s=i[Ri],c=dt(i);if(s!==c&&Na(n,t,e,r,s,a,o),(i[Z]&4)===0)for(let l=Ue;l<i.length;l++){let d=i[l];ac(d[W],d,t,n,r,s)}}function rC(t,n,e,i,a){if(n)a?t.addClass(e,i):t.removeClass(e,i);else{let r=i.indexOf("-")===-1?void 0:gn.DashCase;a==null?t.removeStyle(e,i,r):(typeof a=="string"&&a.endsWith("!important")&&(a=a.slice(0,-10),r|=gn.Important),t.setStyle(e,i,a,r))}}function lu(t,n,e,i,a,r,o,s,c,l,d){let u=Le+i,g=u+a,f=oC(u,g),_=typeof l=="function"?l():l;return f[W]={type:t,blueprint:f,template:e,queries:null,viewQuery:s,declTNode:n,data:f.slice().fill(null,u),bindingStartIndex:u,expandoStartIndex:g,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof r=="function"?r():r,pipeRegistry:typeof o=="function"?o():o,firstChild:null,schemas:c,consts:_,incompleteFirstPass:!1,ssrId:d}}function oC(t,n){let e=[];for(let i=0;i<n;i++)e.push(i<t?null:Dt);return e}function sC(t){let n=t.tView;return n===null||n.incompleteFirstPass?t.tView=lu(1,null,t.template,t.decls,t.vars,t.directiveDefs,t.pipeDefs,t.viewQuery,t.schemas,t.consts,t.id):n}function du(t,n,e,i,a,r,o,s,c,l,d){let u=n.blueprint.slice();return u[Kt]=a,u[Z]=i|4|128|8|64|1024,(l!==null||t&&t[Z]&2048)&&(u[Z]|=2048),Nd(u),u[$e]=u[Jn]=t,u[He]=e,u[Qt]=o||t&&t[Qt],u[De]=s||t&&t[De],u[On]=c||t&&t[On]||null,u[lt]=r,u[gr]=Kw(),u[Mi]=d,u[kd]=l,u[gt]=n.type==2?t[gt]:u,u}function cC(t,n,e){let i=Ot(n,t),a=sC(e),r=t[Qt].rendererFactory,o=mu(t,du(t,a,null,Cb(e),i,n,null,r.createRenderer(i,e),null,null,null));return t[n.index]=o}function Cb(t){let n=16;return t.signals?n=4096:t.onPush&&(n=64),n}function Sb(t,n,e,i){if(e===0)return-1;let a=n.length;for(let r=0;r<e;r++)n.push(i),t.blueprint.push(i),t.data.push(null);return a}function mu(t,n){return t[wa]?t[wd][zt]=n:t[wa]=n,t[wd]=n,n}function v(t=1){Eb(Ce(),U(),Xt()+t,!1)}function Eb(t,n,e,i){if(!i)if((n[Z]&3)===3){let r=t.preOrderCheckHooks;r!==null&&Os(n,r,e)}else{let r=t.preOrderHooks;r!==null&&Fs(n,r,0,e)}ti(e)}var Fr=(function(t){return t[t.None=0]="None",t[t.SignalBased=1]="SignalBased",t[t.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",t})(Fr||{});function $i(t,n,e,i){let a=j(null);try{let[r,o,s]=t.inputs[e],c=null;(o&Fr.SignalBased)!==0&&(c=n[r][Ke]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):s!==null&&(i=s.call(n,i)),t.setInput!==null?t.setInput(n,c,i,e,r):Tg(n,c,r,i)}finally{j(a)}}function Db(t,n,e,i,a){let r=Xt(),o=i&2;try{ti(-1),o&&n.length>Le&&Eb(t,n,Le,!1);let s=o?be.TemplateUpdateStart:be.TemplateCreateStart;ke(s,a,e),e(i,a)}finally{ti(r);let s=o?be.TemplateUpdateEnd:be.TemplateCreateEnd;ke(s,a,e)}}function rc(t,n,e){pC(t,n,e),(e.flags&64)===64&&hC(t,n,e)}function Lr(t,n,e=Ot){let i=n.localNames;if(i!==null){let a=n.index+1;for(let r=0;r<i.length;r+=2){let o=i[r+1],s=o===-1?e(n,t):t[o];t[a++]=s}}}function lC(t,n,e,i){let r=i.get(nb,tb)||e===an.ShadowDom||e===an.ExperimentalIsolatedShadowDom,o=t.selectRootElement(n,r);return dC(o),o}function dC(t){mC(t)}var mC=()=>null;function uC(t){return t==="class"?"className":t==="for"?"htmlFor":t==="formaction"?"formAction":t==="innerHtml"?"innerHTML":t==="readonly"?"readOnly":t==="tabindex"?"tabIndex":t}function Ib(t,n,e,i,a,r){let o=n[W];if(oc(t,o,n,e,i)){pn(t)&&Tb(n,t.index);return}t.type&3&&(e=uC(e)),Nb(t,n,e,i,a,r)}function Nb(t,n,e,i,a,r){if(t.type&3){let o=Ot(t,n);i=r!=null?r(i,t.value||"",e):i,i=Mb(e,o,i),a.setProperty(o,e,i)}else t.type&12}function Mb(t,n,e,i=!1){return t==="src"&&(n.tagName==="IMG"||n.tagName==="VIDEO")?e==null?i?e:"":$t(e):e}function Tb(t,n){let e=Ut(n,t);e[Z]&16||(e[Z]|=64)}function pC(t,n,e){let i=e.directiveStart,a=e.directiveEnd;pn(e)&&cC(n,e,t.data[i+e.componentOffset]),t.firstCreatePass||zs(e,n);let r=e.initialInputs;for(let o=i;o<a;o++){let s=t.data[o],c=Er(n,t,o,e);if(Aa(c,n),r!==null&&bC(n,o-i,c,s,e,r),hn(s)){let l=Ut(e.index,n);l[He]=Er(n,t,o,e)}}}function hC(t,n,e){let i=e.directiveStart,a=e.directiveEnd,r=e.index,o=Mf();try{ti(r);for(let s=i;s<a;s++){let c=t.data[s],l=n[s];ks(s),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&fC(c,l)}}finally{ti(-1),ks(o)}}function fC(t,n){t.hostBindings!==null&&t.hostBindings(1,n)}function uu(t,n){let e=t.directiveRegistry,i=null;if(e)for(let a=0;a<e.length;a++){let r=e[a];gb(n,r.selectors,!1)&&(i??=[],hn(r)?i.unshift(r):i.push(r))}return i}function gC(t,n,e,i,a,r){let o=Ot(t,n);i=Mb(e,o,i,!0),Ab(n[De],o,r,t.value,e,i,a)}function Ab(t,n,e,i,a,r,o){if(r==null)o?.(r,i||"",a),t.removeAttribute(n,a,e);else{let s=o==null?ba(r):o(r,i||"",a);t.setAttribute(n,a,s,e)}}function bC(t,n,e,i,a,r){let o=r[n];if(o!==null)for(let s=0;s<o.length;s+=2){let c=o[s],l=o[s+1];$i(i,e,c,l)}}function pu(t,n,e,i,a){let r=Le+e,o=n[W],s=a(o,n,t,i,e);n[r]=s,Pi(t,!0);let c=t.type===2;return c?(hb(n[De],s,t),(kf()===0||Ca(t))&&Aa(s,n),Cf()):Aa(s,n),Ds()&&(!c||!Js(t))&&su(o,n,s,t),t}function hu(t){let n=t;return Vd()?Bd():(n=n.parent,Pi(n,!1)),n}function vC(t,n){let e=t[On];if(!e)return;let i;try{i=e.get(ni,null)}catch{i=null}i?.(n)}function oc(t,n,e,i,a){let r=t.inputs?.[i],o=t.hostDirectiveInputs?.[i],s=!1;if(o)for(let c=0;c<o.length;c+=2){let l=o[c],d=o[c+1],u=n.data[l];$i(u,e[l],d,a),s=!0}if(r)for(let c of r){let l=e[c],d=n.data[c];$i(d,l,i,a),s=!0}return s}function _C(t,n,e,i,a,r){let o=null,s=null,c=null,l=!1,d=t.directiveToIndex.get(i.type);if(typeof d=="number"?o=d:[o,s,c]=d,s!==null&&c!==null&&t.hostDirectiveInputs&&Object.hasOwn(t.hostDirectiveInputs,a)){let u=t.hostDirectiveInputs[a];for(let g=0;g<u.length;g+=2){let f=u[g];if(f>=s&&f<=c){let _=n.data[f],w=u[g+1];$i(_,e[f],w,r),l=!0}else if(f>c)break}}return o!==null&&Object.hasOwn(i.inputs,a)&&($i(i,e[o],a,r),l=!0),l}function yC(t,n){let e=Ut(n,t),i=e[W];xC(i,e);let a=e[Kt];a!==null&&e[Mi]===null&&(e[Mi]=ib(a,e[On])),ke(be.ComponentStart);try{fu(i,e,e[He])}finally{ke(be.ComponentEnd,e[He])}}function xC(t,n){for(let e=n.length;e<t.blueprint.length;e++)n.push(t.blueprint[e])}function fu(t,n,e){Ss(n);try{let i=t.viewQuery;i!==null&&pm(1,i,e);let a=t.template;a!==null&&Db(t,n,a,1,e),t.firstCreatePass&&(t.firstCreatePass=!1),n[un]?.finishViewCreation(t),t.staticContentQueries&&ab(t,n),t.staticViewQueries&&pm(2,t.viewQuery,e);let r=t.components;r!==null&&wC(n,r)}catch(i){throw t.firstCreatePass&&(t.incompleteFirstPass=!0,t.firstCreatePass=!1),i}finally{n[Z]&=-5,Es()}}function wC(t,n){for(let e=0;e<n.length;e++)yC(t,n[e])}function Pr(t,n,e,i){let a=j(null);try{let r=n.tView,s=t[Z]&4096?4096:16,c=du(t,r,e,s,null,n,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=t[n.index];c[ei]=l;let d=t[un];return d!==null&&(c[un]=d.createEmbeddedView(r)),fu(r,c,e),c}finally{j(a)}}function Ra(t,n){return!n||n.firstChild===null||qg(t)}function Ir(t,n,e,i,a=!1){if(t.type===3){let r=t.firstChild,o=r.next,s=dt(n[r.index]),c=dt(n[o.index]),l=s;for(;l!==null&&(i.push(l),l!==c);)l=l.nextSibling;return i}for(;e!==null;){if(e.type===128){e=a?e.projectionNext:e.next;continue}let r=n[e.index];if(r!==null)if(Ht(r)){let s=r[Ri];s!==r[Kt]&&i.push(dt(r)),r[Z]&4||Rb(r,i),i.push(s)}else i.push(dt(r));let o=e.type;if(o&8)Ir(t,n,e.child,i);else if(o&32){let s=nu(e,n),c;for(;c=s();)i.push(c)}else if(o&16){let s=wb(n,e);if(Array.isArray(s))i.push(...s);else{let c=An(n[gt]);Ir(c[W],c,s,i,!0)}}e=a?e.projectionNext:e.next}return i}function Rb(t,n){for(let e=Ue;e<t.length;e++){let i=t[e],a=i[W].firstChild;a!==null&&Ir(i[W],i,a,n)}}function Ob(t){if(t[Ai]!==null){for(let n of t[Ai])n.impl.addSequence(n);t[Ai].length=0}}var Fb=[];function kC(t){return t[At]??CC(t)}function CC(t){let n=Fb.pop()??Object.create(EC);return n.lView=t,n}function SC(t){t.lView[At]!==t&&(t.lView=null,Fb.push(t))}var EC=ge(P({},hi),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{Li(t.lView)},consumerOnSignalRead(){this.lView[At]=this}});function DC(t){let n=t[At]??Object.create(IC);return n.lView=t,n}var IC=ge(P({},hi),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{let n=An(t.lView);for(;n&&!Lb(n[W]);)n=An(n);n&&Md(n)},consumerOnSignalRead(){this.lView[At]=this}});function Lb(t){return t.type!==2}function Pb(t){if(t[Tn]===null)return;let n=!0;for(;n;){let e=!1;for(let i of t[Tn])if(i.dirty&&(e=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()),t[Tn]===null))return;n=e&&!!(t[Z]&8192)}}var NC=100;function Vb(t,n=0){let i=t[Qt].rendererFactory,a=!1;a||i.begin?.();try{MC(t,n)}finally{a||i.end?.()}}function MC(t,n){let e=zd();try{lr(!0),Sm(t,n);let i=0;for(;vr(t);){if(i===NC)throw new H(103,!1);i++,Sm(t,1)}}finally{lr(e)}}function TC(t,n,e,i){if(Fi(n))return;let a=n[Z],r=!1,o=!1;Ss(n);let s=!0,c=null,l=null;r||(Lb(t)?(l=kC(n),c=Yn(l)):bo()===null?(s=!1,l=DC(n),c=Yn(l)):n[At]&&(Zn(n[At]),n[At]=null));try{Nd(n),Df(t.bindingStartIndex),e!==null&&Db(t,n,e,2,i);let d=(a&3)===3;if(!r)if(d){let f=t.preOrderCheckHooks;f!==null&&Os(n,f,null)}else{let f=t.preOrderHooks;f!==null&&Fs(n,f,0,null),Qd(n,0)}if(o||AC(n),Pb(n),jb(n,0),t.contentQueries!==null&&ab(t,n),!r)if(d){let f=t.contentCheckHooks;f!==null&&Os(n,f)}else{let f=t.contentHooks;f!==null&&Fs(n,f,1),Qd(n,1)}OC(t,n);let u=t.components;u!==null&&Hb(n,u,0);let g=t.viewQuery;if(g!==null&&pm(2,g,i),!r)if(d){let f=t.viewCheckHooks;f!==null&&Os(n,f)}else{let f=t.viewHooks;f!==null&&Fs(n,f,2),Qd(n,2)}if(t.firstUpdatePass===!0&&(t.firstUpdatePass=!1),n[fs]){for(let f of n[fs])f();n[fs]=null}r||(Ob(n),n[Z]&=-73)}catch(d){Bb(n,d,r)}finally{l!==null&&(fi(l,c),s&&SC(l)),Es()}}function Bb(t,n,e){let i=!1,a=n,r=t,o=j(null);try{for(;r!==null;){if(Ht(r)){r=r[$e];continue}let s=r[ka];if(s)try{let c=t[gt][He],l=c?.constructor,d={declarationInstance:c,declarationType:l,caughtBy:s},u=j(null);try{s(Hf(a),d)}finally{j(u)}i=!0;break}catch(c){a=c}r=r[$e]}}finally{j(o)}if(!i)throw e||Li(t),a}function jb(t,n){for(let e=Zg(t);e!==null;e=Kg(e))for(let i=Ue;i<e.length;i++){let a=e[i];zb(a,n)}}function AC(t){for(let n=Zg(t);n!==null;n=Kg(n)){if(!(n[Z]&2))continue;let e=n[Oi];for(let i=0;i<e.length;i++){let a=e[i];Md(a)}}}function RC(t,n,e){ke(be.ComponentStart);let i=Ut(n,t);try{zb(i,e)}finally{ke(be.ComponentEnd,i[He])}}function zb(t,n){vs(t)&&Sm(t,n)}function Sm(t,n){let i=t[W],a=t[Z],r=t[At],o=!!(n===0&&a&16);if(o||=!!(a&64&&n===0),o||=!!(a&1024),o||=!!(r?.dirty&&aa(r)),o||=!1,r&&(r.dirty=!1),t[Z]&=-9217,o)TC(i,t,i.template,t[He]);else if(a&8192){let s=j(null);try{try{Pb(t)}catch(l){Bb(t,l,!1)}jb(t,1);let c=i.components;c!==null&&Hb(t,c,1),Ob(t)}finally{j(s)}}}function Hb(t,n,e){for(let i=0;i<n.length;i++)RC(t,n[i],e)}function OC(t,n){let e=t.hostBindingOpCodes;if(e!==null)try{for(let i=0;i<e.length;i++){let a=e[i];if(a<0)ti(~a);else{let r=a,o=e[++i],s=e[++i];Nf(o,r);let c=n[r];ke(be.HostBindingsUpdateStart,c);try{s(2,c)}finally{ke(be.HostBindingsUpdateEnd,c)}}}}finally{ti(-1)}}function gu(t,n){let e=zd()?64:1088;for(t[Qt].changeDetectionScheduler?.notify(n);t;){t[Z]|=e;let i=An(t);if(Sa(t)&&!i)return t;t=i}return null}function Ub(t,n,e,i){return[t,!0,0,n,null,i,null,e,null,null]}function $b(t,n){let e=Ue+n;if(e<t.length)return t[e]}function Vr(t,n,e,i=!0){let a=n[W];if(FC(a,n,t,e),i){let o=Cm(e,t),s=n[De],c=s.parentNode(t[Ri]);c!==null&&Zk(a,t[lt],s,n,c,o)}let r=n[Mi];r!==null&&r.firstChild!==null&&(r.firstChild=null)}function Gb(t,n){let e=Nr(t,n);return e!==void 0&&ic(e[W],e),e}function Nr(t,n){if(t.length<=Ue)return;let e=Ue+n,i=t[e];if(i){let a=i[ei];a!==null&&a!==t&&ou(a,i),n>0&&(t[e-1][zt]=i[zt]);let r=hr(t,Ue+n);Yk(i[W],i);let o=r[un];o!==null&&o.detachView(r[W]),i[$e]=null,i[zt]=null,i[Z]&=-129}return i}function FC(t,n,e,i){let a=Ue+i,r=e.length;i>0&&(e[a-1][zt]=n),i<r-Ue?(n[zt]=e[a],gd(e,Ue+i,n)):(e.push(n),n[zt]=null),n[$e]=e;let o=n[ei];o!==null&&e!==o&&Wb(o,n);let s=n[un];s!==null&&s.insertView(t),_s(n),n[Z]|=128}function Wb(t,n){let e=t[Oi],i=n[$e];if(Fn(i))t[Z]|=2;else{let a=i[$e][gt];n[gt]!==a&&(t[Z]|=2)}e===null?t[Oi]=[n]:e.push(n)}var ii=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,e=n[W];return Ir(e,n,e.firstChild,[])}constructor(n,e){this._lView=n,this._cdRefInjectingView=e}get context(){return this._lView[He]}set context(n){this._lView[He]=n}get destroyed(){return Fi(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[$e];if(Ht(n)){let e=n[br],i=e?e.indexOf(this):-1;i>-1&&(Nr(n,i),hr(e,i))}this._attachedToViewContainer=!1}ic(this._lView[W],this._lView)}onDestroy(n){ys(this._lView,n)}markForCheck(){gu(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[Z]&=-129}reattach(){_s(this._lView),this._lView[Z]|=128}detectChanges(){this._lView[Z]|=1024,Vb(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new H(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=Sa(this._lView),e=this._lView[ei];e!==null&&!n&&ou(e,this._lView),_b(this._lView[W],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new H(902,!1);this._appRef=n;let e=Sa(this._lView),i=this._lView[ei];i!==null&&!e&&Wb(i,this._lView),_s(this._lView)}};var bt=(()=>{class t{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=LC;constructor(e,i,a){this._declarationLView=e,this._declarationTContainer=i,this.elementRef=a}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,i){return this.createEmbeddedViewImpl(e,i)}createEmbeddedViewImpl(e,i,a){let r=Pr(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:i,dehydratedView:a});return new ii(r)}}return t})();function LC(){return sc(Xe(),U())}function sc(t,n){return t.type&4?new bt(n,t,Pa(t,n)):null}function Wi(t,n,e,i,a){let r=t.data[n];if(r===null)r=PC(t,n,e,i,a),If()&&(r.flags|=32);else if(r.type&64){r.type=e,r.value=i,r.attrs=a;let o=Sf();r.injectorIndex=o===null?-1:o.injectorIndex}return Pi(r,!0),r}function PC(t,n,e,i,a){let r=Pd(),o=Vd(),s=o?r:r&&r.parent,c=t.data[n]=BC(t,s,e,n,i,a);return VC(t,c,r,o),c}function VC(t,n,e,i){t.firstChild===null&&(t.firstChild=n),e!==null&&(i?e.child==null&&n.parent!==null&&(e.child=n):e.next===null&&(e.next=n,n.prev=e))}function BC(t,n,e,i,a,r){let o=n?n.injectorIndex:-1,s=0;return Od()&&(s|=128),{type:e,index:i,insertBeforeIndex:null,injectorIndex:o,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:s,providerIndexes:0,value:a,namespace:Gd(),attrs:r,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function jC(t){let n=t[Cd]??[],i=t[$e][De],a=[];for(let r of n)r.data[eb]!==void 0?a.push(r):zC(r,i);t[Cd]=a}function zC(t,n){let e=0,i=t.firstChild;if(i){let a=t.data[Jg];for(;e<a;){let r=i.nextSibling;pb(n,i,!1),i=r,e++}}}var HC=()=>null,UC=()=>null;function Hs(t,n){return HC(t,n)}function qb(t,n,e){return UC(t,n,e)}var Yb=class{},Je=class{},Se=class{destroyNode=null;static __NG_ELEMENT_ID__=()=>$C()};function $C(){let t=U(),n=Xe(),e=Ut(n.index,t);return(Fn(e)?e:t)[De]}var Zb=(()=>{class t{static \u0275prov=Te({token:t,providedIn:"root",factory:()=>null})}return t})();function Kb(t){return t.debugInfo?.className||t.type.name||null}var Ps={},Us=class{injector;parentInjector;constructor(n,e){this.injector=n,this.parentInjector=e}get(n,e,i){let a=this.injector.get(n,Ps,i);return a!==Ps||e===Ps?a:this.parentInjector.get(n,e,i)}};function Qb(t,n,e){return t[n]=e}function Pt(t,n,e){if(e===Dt)return!1;let i=t[n];return Object.is(i,e)?!1:(t[n]=e,!0)}function Xb(t,n,e,i){let a=Pt(t,n,e);return Pt(t,n+1,i)||a}function Cr(t,n,e){return function i(a){let r=i.__ngNativeEl__;r!==void 0&&Xw(a,r);let o=pn(t)?Ut(t.index,n):n;gu(o,5);let s=n[He],c=pg(n,s,e,a),l=i.__ngNextListenerFn__;for(;l;)c=pg(n,s,l,a)&&c,l=l.__ngNextListenerFn__;return c}}function pg(t,n,e,i){let a=j(null);try{return ke(be.OutputStart,n,e),e(i)!==!1}catch(r){return vC(t,r),!1}finally{ke(be.OutputEnd,n,e),j(a)}}function GC(t,n,e,i,a,r,o,s){let c=Ca(t),l=!1,d=null;if(!i&&c&&(d=qC(n,e,r,t.index)),d!==null){let u=d.__ngLastListenerFn__||d;u.__ngNextListenerFn__=o,d.__ngLastListenerFn__=o,l=!0}else{let u=Ot(t,e),g=i?i(u):u;ek(e,g,r,s),i||(s.__ngNativeEl__=u);let f=a.listen(g,r,s);if(!WC(r)){let _=i?w=>i(dt(w[t.index])):t.index;Jb(_,n,e,r,s,f,!1)}}return l}function WC(t){return t.startsWith("animation")||t.startsWith("transition")}function qC(t,n,e,i){let a=t.cleanup;if(a!=null)for(let r=0;r<a.length-1;r+=2){let o=a[r];if(o===e&&a[r+1]===i){let s=n[xa],c=a[r+2];return s&&s.length>c?s[c]:null}typeof o=="string"&&(r+=2)}return null}function Jb(t,n,e,i,a,r,o){let s=n.firstCreatePass?Ad(n):null,c=Td(e),l=c.length;c.push(a,r),s&&s.push(i,t,l,(l+1)*(o?-1:1))}function hg(t,n,e,i,a){let r=null,o=null,s=null,c=!1,l=t.directiveToIndex.get(e.type);if(typeof l=="number"?r=l:[r,o,s]=l,o!==null&&s!==null&&t.hostDirectiveOutputs&&Object.hasOwn(t.hostDirectiveOutputs,i)){let d=t.hostDirectiveOutputs[i];for(let u=0;u<d.length;u+=2){let g=d[u];if(g>=o&&g<=s)c=!0,$s(t,n,g,d[u+1],i,a);else if(g>s)break}}return Object.hasOwn(e.outputs,i)&&(c=!0,$s(t,n,r,i,i,a)),c}function $s(t,n,e,i,a,r){let o=n[e],s=n[W],l=s.data[e].outputs[i],u=o[l].subscribe(r);Jb(t.index,s,n,a,r,u,!0)}function vn(){YC()}function YC(){let t=U(),n=Ce(),e=Xe();if(n.firstCreatePass&&KC(n,e),e.controlDirectiveIndex===-1)return;jn("NgSignalForms");let i=t[e.controlDirectiveIndex];n.data[e.controlDirectiveIndex].controlDef.create(i,new Gs(t,n,e))}function _n(){ZC()}function ZC(){let t=U(),n=Ce(),e=Vi();if(e.controlDirectiveIndex===-1)return;let i=n.data[e.controlDirectiveIndex].controlDef,a=t[e.controlDirectiveIndex];i.update(a,new Gs(t,n,e))}var Gs=class{lView;tView;tNode;hasPassThrough;constructor(n,e,i){this.lView=n,this.tView=e,this.tNode=i,this.hasPassThrough=!!(i.flags&4096)}get customControl(){return this.tNode.customControlIndex!==-1?this.lView[this.tNode.customControlIndex]:void 0}get nativeElement(){return Ot(this.tNode,this.lView)}get descriptor(){return`<${this.tNode.value}>`}listenToCustomControlOutput(n,e){let i=this.tView.data[this.tNode.customControlIndex];hg(this.tNode,this.lView,i,n,Cr(this.tNode,this.lView,e))}listenToCustomControlModel(n){let e=this.tNode.flags&1024?"valueChange":"checkedChange",i=this.tView.data[this.tNode.customControlIndex];hg(this.tNode,this.lView,i,e,Cr(this.tNode,this.lView,n))}setInputOnDirectives(n,e,i){let a=this.tNode.inputs?.[n],r=this.tNode.hostDirectiveInputs?.[n];if(!a&&!r)return!1;let o=!1;if(a)for(let s of a){if(s===this.tNode.controlDirectiveIndex)continue;let c=this.lView[s],l=this.tView.data[s];(!i||i(gg(c,l,n)))&&($i(l,c,n,e),o=!0)}if(r)for(let s=0;s<r.length;s+=2){let c=r[s];if(c===this.tNode.controlDirectiveIndex)continue;let l=this.lView[c],d=r[s+1],u=this.tView.data[c];(!i||i(gg(l,u,n)))&&($i(u,l,d,e),o=!0)}return o}setCustomControlModelInput(n){let e=this.tView.data[this.tNode.customControlIndex],i=this.tNode.flags&1024?"value":"checked";_C(this.tNode,this.tView,this.lView,e,i,n)}customControlHasInput(n){if(this.tNode.customControlIndex===-1)return!1;let e=this.tView.data[this.tNode.customControlIndex];return(e.signalFormsInputPresence??=this._buildCustomControlInputCache(e))[n]===!0}_buildCustomControlInputCache(n){let e={};for(let i in n.inputs)e[i]=!0;if(n.hostDirectives!==null){let i=[...n.hostDirectives];for(;i.length>0;){let a=i.shift();if(typeof a!="function"){for(let o in a.inputs)e[a.inputs[o]]=!0;let r=fg(a.directive);r!==null&&i.push(...r);continue}for(let r of a()){if(typeof r=="function")continue;if(r.inputs)for(let s=0;s<r.inputs.length;s+=2){let c=r.inputs[s+1]||r.inputs[s];e[c]=!0}let o=fg(r.directive);o!==null&&i.push(...o)}}}return e}};function fg(t){return typeof t=="function"&&"\u0275dir"in t?t.\u0275dir.hostDirectives??null:null}function gg(t,n,e){if(!n.inputs||!Object.hasOwn(n.inputs,e))return;let[i,a]=n.inputs[e];if((a&Fr.SignalBased)!==0){let o=t[i][Ke];return o.value===Xs?void 0:o.value}return t[i]}function KC(t,n,e){for(let a=n.directiveStart;a<n.directiveEnd;a++)if(t.data[a].controlDef){n.controlDirectiveIndex=a;break}if(n.controlDirectiveIndex===-1)return;let i=t.data[n.controlDirectiveIndex].controlDef;if(i.passThroughInput&&(n.inputs?.[i.passThroughInput]?.length??0)>1){n.flags|=4096;return}QC(t,n)}function QC(t,n){for(let e=n.directiveStart;e<n.directiveEnd;e++){let i=t.data[e];if(!(n.directiveToIndex&&!n.directiveToIndex.has(i.type))){if(bg(i,"value")){n.flags|=1024,n.customControlIndex=e;return}if(bg(i,"checked")){n.flags|=2048,n.customControlIndex=e;return}}}if(n.hostDirectiveInputs!==null&&n.hostDirectiveOutputs!==null&&n.directiveToIndex!==null){let e=(i,a)=>{let r=n.hostDirectiveInputs[i],o=n.hostDirectiveOutputs[i+"Change"];if(!r||!o)return!1;for(let s=0;s<r.length;s+=2){let c=r[s];for(let l=0;l<o.length;l+=2){let d=o[l];if(c===d)for(let u of n.directiveToIndex.values()){if(!Array.isArray(u))continue;let[g,f,_]=u;if(c>=f&&c<=_)return n.flags|=a,n.customControlIndex=g,!0}}}return!1};if(e("value",1024)||e("checked",2048))return}}function bg(t,n){return XC(t,n)&&JC(t,n+"Change")}function XC(t,n){return n in t.inputs}function JC(t,n){return n in t.outputs}var Em=Symbol("BINDING");var qi=new y("");function Ws(t,n,e){let i=e?t.styles:null,a=e?t.classes:null,r=0;if(n!==null)for(let o=0;o<n.length;o++){let s=n[o];if(typeof s=="number")r=s;else if(r==1)a=os(a,s);else if(r==2){let c=s,l=n[++o];i=os(i,c+": "+l+";")}}e?t.styles=i:t.stylesWithoutHost=i,e?t.classes=a:t.classesWithoutHost=a}function Ae(t,n=0){let e=U();if(e===null)return ce(t,n);let i=Xe();return Ug(i,e,st(t),n)}function ev(t,n,e,i,a){let r=i===null?null:{"":-1},o=a(t,e);if(o!==null){let s=o,c=null,l=null;for(let d of o)if(d.resolveHostDirectives!==null){[s,c,l]=d.resolveHostDirectives(o);break}nS(t,n,e,s,r,c,l)}r!==null&&i!==null&&eS(e,i,r)}function eS(t,n,e){let i=t.localNames=[];for(let a=0;a<n.length;a+=2){let r=e[n[a+1]];if(r==null)throw new H(-301,!1);i.push(n[a],r)}}function tS(t,n,e){n.componentOffset=e,(t.components??=[]).push(n.index)}function nS(t,n,e,i,a,r,o){let s=i.length,c=null;for(let g=0;g<s;g++){let f=i[g];c===null&&hn(f)&&(c=f,tS(t,e,g)),lm(zs(e,n),t,f.type)}cS(e,t.data.length,s),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let g=0;g<s;g++){let f=i[g];f.providersResolver&&f.providersResolver(f)}let l=!1,d=!1,u=Sb(t,n,s,null);s>0&&(e.directiveToIndex=new Map);for(let g=0;g<s;g++){let f=i[g];if(e.mergedAttrs=Ta(e.mergedAttrs,f.hostAttrs),aS(t,e,n,u,f),sS(u,f,a),o!==null&&o.has(f)){let[w,A]=o.get(f);e.directiveToIndex.set(f.type,[u,w+e.directiveStart,A+e.directiveStart])}else(r===null||!r.has(f))&&e.directiveToIndex.set(f.type,u);f.contentQueries!==null&&(e.flags|=4),(f.hostBindings!==null||f.hostAttrs!==null||f.hostVars!==0)&&(e.flags|=64);let _=f.type.prototype;!l&&(_.ngOnChanges||_.ngOnInit||_.ngDoCheck)&&((t.preOrderHooks??=[]).push(e.index),l=!0),!d&&(_.ngOnChanges||_.ngDoCheck)&&((t.preOrderCheckHooks??=[]).push(e.index),d=!0),u++}iS(t,e,r)}function iS(t,n,e){for(let i=n.directiveStart;i<n.directiveEnd;i++){let a=t.data[i];if(e===null||!e.has(a))vg(0,n,a,i),vg(1,n,a,i),yg(n,i,!1);else{let r=e.get(a);_g(0,n,r,i),_g(1,n,r,i),yg(n,i,!0)}}}function vg(t,n,e,i){let a=t===0?e.inputs:e.outputs;for(let r in a)if(Object.hasOwn(a,r)){let o;t===0?o=n.inputs??={}:o=n.outputs??={},o[r]??=[],o[r].push(i),tv(n,r)}}function _g(t,n,e,i){let a=t===0?e.inputs:e.outputs;for(let r in a)if(Object.hasOwn(a,r)){let o=a[r],s;t===0?s=n.hostDirectiveInputs??={}:s=n.hostDirectiveOutputs??={},s[o]??=[],s[o].push(i,r),tv(n,o)}}function tv(t,n){n==="class"?t.flags|=8:n==="style"&&(t.flags|=16)}function yg(t,n,e){let{attrs:i,inputs:a,hostDirectiveInputs:r}=t;if(i===null||!e&&a===null||e&&r===null||tu(t)){t.initialInputs??=[],t.initialInputs.push(null);return}let o=null,s=0;for(;s<i.length;){let c=i[s];if(c===0){s+=4;continue}else if(c===5){s+=2;continue}else if(typeof c=="number")break;if(!e&&Object.hasOwn(a,c)){let l=a[c];for(let d of l)if(d===n){o??=[],o.push(c,i[s+1]);break}}else if(e&&Object.hasOwn(r,c)){let l=r[c];for(let d=0;d<l.length;d+=2)if(l[d]===n){o??=[],o.push(l[d+1],i[s+1]);break}}s+=2}t.initialInputs??=[],t.initialInputs.push(o)}function aS(t,n,e,i,a){t.data[i]=a;let r=a.factory||(a.factory=Ci(a.type,!0)),o=new Ui(r,hn(a),Ae,null);t.blueprint[i]=o,e[i]=o,rS(t,n,i,Sb(t,e,a.hostVars,Dt),a)}function rS(t,n,e,i,a){let r=a.hostBindings;if(r){let o=t.hostBindingOpCodes;o===null&&(o=t.hostBindingOpCodes=[]);let s=~n.index;oS(o)!=s&&o.push(s),o.push(e,i,r)}}function oS(t){let n=t.length;for(;n>0;){let e=t[--n];if(typeof e=="number"&&e<0)return e}return 0}function sS(t,n,e){if(e){if(n.exportAs)for(let i=0;i<n.exportAs.length;i++)e[n.exportAs[i]]=t;hn(n)&&(e[""]=t)}}function cS(t,n,e){t.flags|=1,t.directiveStart=n,t.directiveEnd=n+e,t.providerIndexes=n}function bu(t,n,e,i,a,r,o,s){let c=n[W],l=c.consts,d=Ft(l,o),u=Wi(c,t,e,i,d);return r&&ev(c,n,u,Ft(l,s),a),u.mergedAttrs=Ta(u.mergedAttrs,u.attrs),u.attrs!==null&&Ws(u,u.attrs,!1),u.mergedAttrs!==null&&Ws(u,u.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,u),u}function vu(t,n){Og(t,n),Sd(n)&&t.queries.elementEnd(n)}function lS(t,n,e,i,a,r){let o=n.consts,s=Ft(o,a),c=Wi(n,t,e,i,s);if(c.mergedAttrs=Ta(c.mergedAttrs,c.attrs),r!=null){let l=Ft(o,r);c.localNames=[];for(let d=0;d<l.length;d+=2)c.localNames.push(l[d],-1)}return c.attrs!==null&&Ws(c,c.attrs,!1),c.mergedAttrs!==null&&Ws(c,c.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,c),c}var nv=typeof ShadowRoot<"u",dS=typeof Document<"u";function mS(t){return Object.keys(t).map(n=>{let[e,i,a]=t[n],r={propName:e,templateName:n,isSignal:(i&Fr.SignalBased)!==0};return a&&(r.transform=a),r})}function uS(t){return Object.keys(t).map(n=>({propName:t[n],templateName:n}))}function pS(t,n,e){let i=n instanceof ct?n:n?.injector;return i&&t.getStandaloneInjector!==null&&(i=t.getStandaloneInjector(i)||i),i?new Us(e,i):e}function hS(t){let n=t.get(Je,null);if(n===null)throw new H(407,!1);let e=t.get(Zb,null),i=t.get(Rn,null),a=t.get(bn,null,{optional:!0});return{rendererFactory:n,sanitizer:e,changeDetectionScheduler:i,ngReflect:!1,tracingService:a}}function fS(t,n,e){let i=iv(t);return mb(n,i,i==="svg"?Ed:i==="math"?bf:e)}function gS(t){if((t&&"localName"in t&&typeof t.localName=="string"?t.localName:t?.tagName)?.toLowerCase()==="script")throw new H(905,!1)}function iv(t){return(t.selectors[0][0]||"div").toLowerCase()}var Oa=class{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=mS(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=uS(this.componentDef.outputs),this.cachedOutputs}constructor(n,e){this.componentDef=n,this.ngModule=e,this.componentType=n.type,this.selector=Bk(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!e}create(n,e,i,a,r,o,s,c){ke(be.DynamicComponentStart);let l=j(null);try{let d=this.componentDef,u=pS(d,a||this.ngModule,n),g=hS(u),f=g.tracingService;return f&&f.componentCreate?f.componentCreate(Kb(d),()=>this.createComponentRef(g,u,e,i,r,o,s,c)):this.createComponentRef(g,u,e,i,r,o,s,c)}finally{j(l)}}createComponentRef(n,e,i,a,r,o,s,c){let l=this.componentDef,d=bS(a,l,o,r,c),u=n.rendererFactory.createRenderer(null,l),g=a?lC(u,a,l.encapsulation,e):fS(l,u,s??null);gS(g);let f=e.get(qi,null),_=vS(g,()=>e.get(ee,null)??Xg());f&&f.addHost(_);let w=o?.some(xg)||r?.some(fe=>typeof fe!="function"&&fe.bindings.some(xg)),A=du(null,d,null,512|Cb(l),null,null,n,u,e,null,ib(g,e,!0));f&&nv&&_ instanceof ShadowRoot&&ys(A,()=>{f.removeHost(_)}),A[Le]=g,Ss(A);let ie=null;try{let fe=bu(Le,A,2,"#host",()=>d.directiveRegistry,!0,0);hb(u,g,fe),Aa(g,A),rc(d,A,fe),qm(d,fe,A),vu(d,fe),i!==void 0&&yS(fe,this.ngContentSelectors,i),ie=Ut(fe.index,A),A[He]=ie[He],fu(d,A,null)}catch(fe){throw ie!==null&&mm(ie),mm(A),fe}finally{ke(be.DynamicComponentEnd),Es()}return new qs(this.componentType,A,!!w)}};function bS(t,n,e,i,a){let r=t?["ng-version","22.2.5"]:jk(n.selectors[0]),o=null,s=null,c=0;if(e)for(let u of e)c+=u[Em].requiredVars,u.create&&(u.targetIdx=0,(o??=[]).push(u)),u.update&&(u.targetIdx=0,(s??=[]).push(u));if(i)for(let u=0;u<i.length;u++){let g=i[u];if(typeof g!="function")for(let f of g.bindings){c+=f[Em].requiredVars;let _=u+1;f.create&&(f.targetIdx=_,(o??=[]).push(f)),f.update&&(f.targetIdx=_,(s??=[]).push(f))}}let l=[n];if(i)for(let u of i){let g=typeof u=="function"?u:u.type,f=ls(g);l.push(f)}return lu(0,null,_S(o,s),1,c,l,null,null,null,[r],null)}function vS(t,n){let e=t.getRootNode?.();return dS&&e instanceof Document?e.head:e&&nv&&e instanceof ShadowRoot?e:n().head}function _S(t,n){return!t&&!n?null:e=>{if(e&1&&t)for(let i of t)i.create();if(e&2&&n)for(let i of n)i.update()}}function xg(t){let n=t[Em].kind;return n==="input"||n==="twoWay"}var qs=class extends Yb{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,e,i){super(),this._rootLView=e,this._hasInputBindings=i,this._tNode=gs(e[W],Le),this.location=Pa(this._tNode,e),this.instance=Ut(this._tNode.index,e)[He],this.hostView=this.changeDetectorRef=new ii(e,void 0),this.componentType=n}setInput(n,e){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),e))return;let a=this._rootLView,r=oc(i,a[W],a,n,e);this.previousInputValues.set(n,e);let o=Ut(i.index,a);gu(o,1)}get injector(){return new Pn(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function yS(t,n,e){let i=t.projection=[];for(let a=0;a<n.length;a++){let r=e[a];i.push(r!=null&&r.length?Array.from(r):null)}}var Vt=(()=>{class t{static __NG_ELEMENT_ID__=xS}return t})();function xS(){let t=Xe();return av(t,U())}var Dm=class t extends Vt{_lContainer;_hostTNode;_hostLView;constructor(n,e,i){super(),this._lContainer=n,this._hostTNode=e,this._hostLView=i}get element(){return Pa(this._hostTNode,this._hostLView)}get injector(){return new Pn(this._hostTNode,this._hostLView)}get parentInjector(){let n=Um(this._hostTNode,this._hostLView);if(Pg(n)){let e=js(n,this._hostLView),i=Bs(n),a=e[W].data[i+8];return new Pn(a,e)}else return new Pn(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let e=wg(this._lContainer);return e!==null&&e[n]||null}get length(){return this._lContainer.length-Ue}createEmbeddedView(n,e,i){let a,r,o;typeof i=="number"?a=i:i!=null&&(a=i.index,r=i.injector,o=i.onError);let s=Hs(this._lContainer,n.ssrId),c=n.createEmbeddedViewImpl(e||{},r,s);return o&&(c._lView[ka]=o),this.insertImpl(c,a,Ra(this._hostTNode,s)),c}createComponent(n,e,i,a,r,o,s){let c,l,d=e||{};c=d.index,i=d.injector,a=d.projectableNodes,r=d.environmentInjector||d.ngModuleRef,o=d.directives,s=d.bindings,l=d.onError;let u=new Oa(Qn(n)),g=i||this.parentInjector;if(!r&&u.ngModule==null){let fe=this.parentInjector.get(ct,null);fe&&(r=fe)}let f=Qn(u.componentType??{}),_=Hs(this._lContainer,f?.id??null),w=_?.firstChild??null,A=u.create(g,a,w,r,o,s,this._getHostElementNamespace());return l&&(A.hostView._lView[ka]=l),this.insertImpl(A.hostView,c,Ra(this._hostTNode,_)),A}_getHostElementNamespace(){if(this._hostTNode.type&2){let n=this._hostTNode.parent??this._hostLView[lt];return n!==null&&n.type&2&&typeof n.value=="string"&&n.value.toLowerCase()==="foreignobject"?null:n?.namespace??null}return this._hostTNode.namespace}insert(n,e){return this.insertImpl(n,e,!0)}insertImpl(n,e,i){let a=n._lView;if(_f(a)){let s=this.indexOf(n);if(s!==-1)this.detach(s);else{let c=a[$e],l=new t(c,c[lt],c[$e]);l.detach(l.indexOf(n))}}let r=this._adjustIndex(e),o=this._lContainer;return Vr(o,a,r,i),n.attachToViewContainerRef(),gd(em(o),r,n),n}move(n,e){return this.insert(n,e)}indexOf(n){let e=wg(this._lContainer);return e!==null?e.indexOf(n):-1}remove(n){let e=this._adjustIndex(n,-1),i=Nr(this._lContainer,e);i&&(hr(em(this._lContainer),e),ic(i[W],i))}detach(n){let e=this._adjustIndex(n,-1),i=Nr(this._lContainer,e);return i&&hr(em(this._lContainer),e)!=null?new ii(i):null}_adjustIndex(n,e=0){return n??this.length+e}};function wg(t){return t[br]}function em(t){return t[br]||(t[br]=[])}function av(t,n){let e,i=n[t.index];return Ht(i)?e=i:(e=Ub(i,n,null,t),n[t.index]=e,mu(n,e)),kS(e,n,t,i),new Dm(e,t,n)}function wS(t,n){let e=t[De],i=e.createComment(""),a=Ot(n,t),r=e.parentNode(a);return Hi(e,r,i,e.nextSibling(a),!1),i}var kS=ES,CS=()=>!1;function SS(t,n,e){return CS(t,n,e)}function ES(t,n,e,i){if(t[Ri])return;let a;e.type&8?a=dt(i):a=wS(n,e),t[Ri]=a}var Im=class t{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new t(this.queryList)}setDirty(){this.queryList.setDirty()}},Nm=class t{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let e=n.queries;if(e!==null){let i=n.contentQueries!==null?n.contentQueries[0]:e.length,a=[];for(let r=0;r<i;r++){let o=e.getByIndex(r),s=this.queries[o.indexInDeclarationView];a.push(s.clone())}return new t(a)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let e=0;e<this.queries.length;e++)yu(n,e).matches!==null&&this.queries[e].setDirty()}},Ys=class{flags;read;predicate;constructor(n,e,i=null){this.flags=e,this.read=i,typeof n=="string"?this.predicate=TS(n):this.predicate=n}},Mm=class t{queries;constructor(n=[]){this.queries=n}elementStart(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(n,e)}elementEnd(n){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(n)}embeddedTView(n){let e=null;for(let i=0;i<this.length;i++){let a=e!==null?e.length:0,r=this.getByIndex(i).embeddedTView(n,a);r&&(r.indexInDeclarationView=i,e!==null?e.push(r):e=[r])}return e!==null?new t(e):null}template(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].template(n,e)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},Tm=class t{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,e=-1){this.metadata=n,this._declarationNodeIndex=e}elementStart(n,e){this.isApplyingToNode(e)&&this.matchTNode(n,e)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,e){this.elementStart(n,e)}embeddedTView(n,e){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,e),new t(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,i=n.parent;for(;i!==null&&i.type&8&&i.index!==e;)i=i.parent;return e===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(n,e){let i=this.metadata.predicate;if(Array.isArray(i))for(let a=0;a<i.length;a++){let r=i[a];this.matchTNodeWithReadOption(n,e,DS(e,r)),this.matchTNodeWithReadOption(n,e,Ls(e,n,r,!1,!1))}else i===bt?e.type&4&&this.matchTNodeWithReadOption(n,e,-1):this.matchTNodeWithReadOption(n,e,Ls(e,n,i,!1,!1))}matchTNodeWithReadOption(n,e,i){if(i!==null){let a=this.metadata.read;if(a!==null)if(a===L||a===Vt||a===X||a===bt&&e.type&4)this.addMatch(e.index,-2);else{let r=Ls(e,n,a,!1,!1);r!==null&&this.addMatch(e.index,r)}else this.addMatch(e.index,i)}}addMatch(n,e){this.matches===null?this.matches=[n,e]:this.matches.push(n,e)}};function DS(t,n){let e=t.localNames;if(e!==null){for(let i=0;i<e.length;i+=2)if(e[i]===n)return e[i+1]}return null}function IS(t,n){return t.type&11?Pa(t,n):t.type&4?sc(t,n):null}function NS(t,n,e,i){return e===-1?IS(n,t):e===-2?MS(t,n,i):Er(t,t[W],e,n)}function MS(t,n,e){if(e===L)return Pa(n,t);if(e===bt)return sc(n,t);if(e===Vt)return av(n,t);if(e===X)return new Pn(n,t)}function rv(t,n,e,i){let a=n[un].queries[i];if(a.matches===null){let r=t.data,o=e.matches,s=[];for(let c=0;o!==null&&c<o.length;c+=2){let l=o[c];if(l<0)s.push(null);else{let d=r[l];s.push(NS(n,d,o[c+1],e.metadata.read))}}a.matches=s}return a.matches}function Am(t,n,e,i){let a=t.queries.getByIndex(e),r=a.matches;if(r!==null){let o=rv(t,n,a,e);for(let s=0;s<r.length;s+=2){let c=r[s];if(c>0)i.push(o[s/2]);else{let l=r[s+1],d=n[-c];for(let u=Ue;u<d.length;u++){let g=d[u];g[ei]===g[$e]&&Am(g[W],g,l,i)}if(d[Oi]!==null){let u=d[Oi];for(let g=0;g<u.length;g++){let f=u[g];Am(f[W],f,l,i)}}}}}return i}function _u(t,n){return t[un].queries[n].queryList}function ov(t,n,e){let i=new nn((e&4)===4);return wf(t,n,i,i.destroy),(n[un]??=new Nm).queries.push(new Im(i))-1}function sv(t,n,e){let i=Ce();return i.firstCreatePass&&(lv(i,new Ys(t,n,e),-1),(n&2)===2&&(i.staticViewQueries=!0)),ov(i,U(),n)}function cv(t,n,e,i){let a=Ce();if(a.firstCreatePass){let r=Xe();lv(a,new Ys(n,e,i),r.index),AS(a,t),(e&2)===2&&(a.staticContentQueries=!0)}return ov(a,U(),e)}function TS(t){return t.split(",").map(n=>n.trim())}function lv(t,n,e){t.queries===null&&(t.queries=new Mm),t.queries.track(new Tm(n,e))}function AS(t,n){let e=t.contentQueries||(t.contentQueries=[]),i=e.length?e[e.length-1]:-1;n!==i&&e.push(t.queries.length-1,n)}function yu(t,n){return t.queries.getByIndex(n)}function dv(t,n){let e=t[W],i=yu(e,n);return i.crossesNgTemplate?Am(e,t,n,[]):rv(e,t,i,n)}function mv(t,n,e){let i,a=tr(()=>{i._dirtyCounter();let r=RS(i,t);if(n&&r===void 0)throw new H(-951,!1);return r});return i=a[Ke],i._dirtyCounter=ae(0),i._flatValue=void 0,a}function xu(t){return mv(!0,!1,t)}function wu(t){return mv(!0,!0,t)}function uv(t,n){let e=t[Ke];e._lView=U(),e._queryIndex=n,e._queryList=_u(e._lView,n),e._queryList.onDirty(()=>e._dirtyCounter.update(i=>i+1))}function RS(t,n){let e=t._lView,i=t._queryIndex;if(e===void 0||i===void 0||e[Z]&4)return n?void 0:ft;let a=_u(e,i),r=dv(e,i);return a.reset(r,Wg),n?a.first:a._changesDetected||t._flatValue===void 0?t._flatValue=a.toArray():t._flatValue}function Yi(t){return!!t&&typeof t.then=="function"}function ku(t){return!!t&&typeof t.subscribe=="function"}var ai=class{};var Mr=class extends ai{injector;instance=null;constructor(n){super();let e=new Ei([...n.providers,{provide:ai,useValue:this}],n.parent||ya(),n.debugName,new Set(["environment"]));this.injector=e,n.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function pv(t,n,e=null){return new Mr({providers:t,parent:n,debugName:e,runEnvironmentInitializers:!0}).injector}var OS=(()=>{class t{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let i=_d(!1,e.type),a=i.length>0?pv([i],this._injector,""):null;this.cachedInjectors.set(e,a)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=Te({token:t,providedIn:"environment",factory:()=>new t(ce(ct))})}return t})();function B(t){return Ar(()=>{let n=hv(t),e=ge(P({},n),{decls:t.decls,vars:t.vars,template:t.template,consts:t.consts||null,ngContentSelectors:t.ngContentSelectors,onPush:t.changeDetection!==Gm.Eager,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&t.dependencies||null,getStandaloneInjector:n.standalone?a=>a.get(OS).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:t.signals??!1,data:t.data||{},encapsulation:t.encapsulation||an.Emulated,styles:t.styles||ft,_:null,schemas:t.schemas||null,tView:null,id:""});n.standalone&&jn("NgStandalone"),fv(e);let i=t.dependencies;return e.directiveDefs=kg(i,FS),e.pipeDefs=kg(i,ef),e.id=VS(e),e})}function FS(t){return Qn(t)||ls(t)}function q(t){return Ar(()=>({type:t.type,bootstrap:t.bootstrap||ft,declarations:t.declarations||ft,imports:t.imports||ft,exports:t.exports||ft,transitiveCompileScopes:null,schemas:t.schemas||null,id:t.id||null}))}function LS(t,n){if(t==null)return Xn;let e={};for(let i in t)if(Object.hasOwn(t,i)){let a=t[i],r,o,s,c;Array.isArray(a)?(s=a[0],r=a[1],o=a[2]??r,c=a[3]||null):(r=a,o=a,s=Fr.None,c=null),e[r]=[i,s,c],n[r]=o}return e}function PS(t){if(t==null)return Xn;let n={};for(let e in t)Object.hasOwn(t,e)&&(n[t[e]]=e);return n}function D(t){return Ar(()=>{let n=hv(t);return fv(n),n})}function hv(t){let n={};return{type:t.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:t.hostBindings||null,hostVars:t.hostVars||0,hostAttrs:t.hostAttrs||null,contentQueries:t.contentQueries||null,declaredInputs:n,inputConfig:t.inputs||Xn,exportAs:t.exportAs||null,standalone:t.standalone??!0,signals:t.signals===!0,selectors:t.selectors||ft,viewQuery:t.viewQuery||null,features:t.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,signalFormsInputPresence:null,inputs:LS(t.inputs,n),outputs:PS(t.outputs),debugInfo:null}}function fv(t){t.features?.forEach(n=>n(t))}function kg(t,n){return t?()=>{let e=typeof t=="function"?t():t,i=[];for(let a of e){let r=n(a);r!==null&&i.push(r)}return i}:null}function VS(t){let n=0,e=typeof t.consts=="function"?"":t.consts,i=[t.selectors,t.ngContentSelectors,t.hostVars,t.hostAttrs,e,t.vars,t.decls,t.encapsulation,t.standalone,t.signals,t.exportAs,JSON.stringify(t.inputs),JSON.stringify(t.outputs),Object.getOwnPropertyNames(t.type.prototype),!!t.contentQueries,!!t.viewQuery];for(let r of i.join("|"))n=Math.imul(31,n)+r.charCodeAt(0)<<0;return n+=2147483648,"c"+n}var gv=new y("");var Cu=(()=>{class t{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,i)=>{this.resolve=e,this.reject=i});appInits=m(gv,{optional:!0})??[];injector=m(X);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let a of this.appInits){let r=hs(this.injector,a);if(Yi(r))e.push(r);else if(ku(r)){let o=new Promise((s,c)=>{r.subscribe({complete:s,error:c})});e.push(o)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{i()}).catch(a=>{this.reject(a)}),e.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})}return t})();function Su(t){return n=>{n.controlDef={create:(e,i)=>{e?.\u0275ngControlCreate(i)},update:(e,i)=>{e?.\u0275ngControlUpdate?.(i)},passThroughInput:t}}}function BS(t){return Object.getPrototypeOf(t.prototype).constructor}function le(t){let n=BS(t.type),e=!0,i=[t];for(;n&&n!==Function.prototype&&n!==Object.prototype;){let a,r=Object.hasOwn(n,ur)?n[ur]:void 0,o=Object.hasOwn(n,pr)?n[pr]:void 0;if(hn(t))a=r??o;else{if(r)throw new H(903,!1);a=o}if(a){if(e){i.push(a);let c=t;c.inputs=tm(t.inputs),c.declaredInputs=tm(t.declaredInputs),c.outputs=tm(t.outputs);let l=a.hostBindings;l&&$S(t,l);let d=a.viewQuery,u=a.contentQueries;if(d&&HS(t,d),u&&US(t,u),jS(t,a),Jh(t.outputs,a.outputs),hn(a)&&a.data.animation){let g=t.data;g.animation=(g.animation||[]).concat(a.data.animation)}}let s=a.features;if(s)for(let c=0;c<s.length;c++){let l=s[c];l&&l.ngInherit&&l(t),l===le&&(e=!1)}}n=Object.getPrototypeOf(n)}zS(i)}function jS(t,n){for(let e in n.inputs){if(!Object.hasOwn(n.inputs,e)||Object.hasOwn(t.inputs,e))continue;let i=n.inputs[e];i!==void 0&&(t.inputs[e]=i,t.declaredInputs[e]=n.declaredInputs[e])}}function zS(t){let n=0,e=null;for(let i=t.length-1;i>=0;i--){let a=t[i];a.hostVars=n+=a.hostVars,a.hostAttrs=Ta(a.hostAttrs,e=Ta(e,a.hostAttrs))}}function tm(t){return t===Xn?{}:t===ft?[]:t}function HS(t,n){let e=t.viewQuery;e?t.viewQuery=(i,a)=>{n(i,a),e(i,a)}:t.viewQuery=n}function US(t,n){let e=t.contentQueries;e?t.contentQueries=(i,a,r)=>{n(i,a,r),e(i,a,r)}:t.contentQueries=n}function $S(t,n){let e=t.hostBindings;e?t.hostBindings=(i,a)=>{n(i,a),e(i,a)}:t.hostBindings=n}function bv(t,n,e,i,a,r,o,s){if(e.firstCreatePass){t.mergedAttrs=Ta(t.mergedAttrs,t.attrs);let d=t.tView=lu(2,t,a,r,o,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,t),d.queries=e.queries.embeddedTView(t))}s&&(t.flags|=s),Pi(t,!1);let c=WS(e,n,t,i);Ds()&&su(e,n,c,t),Aa(c,n);let l=Ub(c,n,c,t);n[i+Le]=l,mu(n,l),SS(l,t,n)}function GS(t,n,e,i,a,r,o,s,c,l,d){let u=e+Le,g;return n.firstCreatePass?(g=Wi(n,u,4,o||null,s||null),xs()&&ev(n,t,g,Ft(n.consts,l),uu),Og(n,g)):g=n.data[u],bv(g,t,n,e,i,a,r,c),Ca(g)&&rc(n,t,g),l!=null&&Lr(t,g,d),g}function Fa(t,n,e,i,a,r,o,s,c,l,d){let u=e+Le,g;if(n.firstCreatePass){if(g=Wi(n,u,4,o||null,s||null),l!=null){let f=Ft(n.consts,l);g.localNames=[];for(let _=0;_<f.length;_+=2)g.localNames.push(f[_],-1)}}else g=n.data[u];return bv(g,t,n,e,i,a,r,c),l!=null&&Lr(t,g,d),g}function It(t,n,e,i,a,r,o,s){let c=U(),l=Ce(),d=Ft(l.consts,r);return GS(c,l,t,n,e,i,a,d,void 0,o,s),It}function cc(t,n,e,i,a,r,o,s){let c=U(),l=Ce(),d=Ft(l.consts,r);return Fa(c,l,t,n,e,i,a,d,void 0,o,s),cc}var WS=qS;function qS(t,n,e,i){return yr(!0),n[De].createComment("")}var Eu=new y("");var Du=new y("");function vv(){Tl(t=>{let n="";throw new H(-600,n)})}var YS=10;var rn=(()=>{class t{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=m(ni);afterRenderManager=m(nc);zonelessEnabled=m(wr);rootEffectScheduler=m(Ns);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new E;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=m(Ea);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(Qe(e=>!e))}constructor(){m(bn,{optional:!0})}whenStable(){let e;return new Promise(i=>{e=this.isStable.subscribe({next:a=>{a&&i()}})}).finally(()=>{e.unsubscribe()})}_injector=m(ct);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,i){return this.bootstrapImpl(e,i)}bootstrapImpl(e,i,a=X.NULL){return this._injector.get(N).run(()=>{if(ke(be.BootstrapComponentStart),!this._injector.get(Cu).done){let ie="";throw new H(405,ie)}let s=Qn(e),c=this._injector.get(ai),l=new Oa(s,c);this.componentTypes.push(e);let{hostElement:d,directives:u,bindings:g}=ZS(i),f=d||l.selector,_=l.create(a,[],f,c.injector,u,g),w=_.location.nativeElement,A=_.injector.get(Eu,null);return A?.registerApplication(w),_.onDestroy(()=>{this.detachView(_.hostView),Sr(this.components,_),A?.unregisterApplication(w)}),this._loadComponent(_),ke(be.BootstrapComponentEnd,_),_})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){ke(be.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(tc.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw ke(be.ChangeDetectionEnd),new H(101,!1);let e=j(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,j(e),this.afterTick.next(),ke(be.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(Je,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<YS;){ke(be.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{ke(be.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:a}of this.allViews){if(!i&&!vr(a))continue;let r=i&&!this.zonelessEnabled?0:1;Vb(a,r),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>vr(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let i=e;this._views.push(i),i.attachToAppRef(this)}detachView(e){let i=e;Sr(this._views,i),i.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(a){this.internalErrorHandler(a)}this.components.push(e),this._injector.get(Du,[]).forEach(a=>a(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>Sr(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new H(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})}return t})();function ZS(t){return t===void 0||typeof t=="string"||t instanceof Element?{hostElement:t}:t}function Sr(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function lc(t,n){let e=U(),i=Ln();if(Pt(e,i,n)){let a=Ce(),r=Vi();if(oc(r,a,e,t,n))pn(r)&&Tb(e,r.index);else{let s=Ot(r,e);Ab(e[De],s,null,r.value,t,n,null)}}return lc}function K(t,n,e,i){let a=U(),r=Ln();if(Pt(a,r,n)){let o=Ce(),s=Vi();gC(s,a,t,n,e,i)}return K}var Rm=class{destroy(n){}updateValue(n,e){}swap(n,e){let i=Math.min(n,e),a=Math.max(n,e),r=this.detach(a);if(a-i>1){let o=this.detach(i);this.attach(i,r),this.attach(a,o)}else this.attach(i,r)}move(n,e){this.attach(e,this.detach(n))}};function nm(t,n,e,i,a){return t===e&&Object.is(n,i)?1:Object.is(a(t,n),a(e,i))?-1:0}function KS(t,n,e,i){let a,r,o=0,s=t.length-1,c=void 0;if(Array.isArray(n)){j(i);let l=n.length-1;for(j(null);o<=s&&o<=l;){let d=t.at(o),u=n[o],g=nm(o,d,o,u,e);if(g!==0){g<0&&t.updateValue(o,u),o++;continue}let f=t.at(s),_=n[l],w=nm(s,f,l,_,e);if(w!==0){w<0&&t.updateValue(s,_),s--,l--;continue}let A=e(o,d),ie=e(s,f),fe=e(o,u);if(Object.is(fe,ie)){let cn=e(l,_);Object.is(cn,A)?(t.swap(o,s),t.updateValue(s,_),l--,s--):t.move(s,o),t.updateValue(o,u),o++;continue}if(a??=new Zs,r??=Sg(t,o,s,e),Om(t,a,o,fe))t.updateValue(o,u),o++,s++;else if(r.has(fe))a.set(A,t.detach(o)),s--;else{let cn=t.create(o,n[o]);t.attach(o,cn),o++,s++}}for(;o<=l;)Cg(t,a,e,o,n[o]),o++}else if(n!=null){j(i);let l=n[Symbol.iterator]();j(null);let d=l.next();for(;!d.done&&o<=s;){let u=t.at(o),g=d.value,f=nm(o,u,o,g,e);if(f!==0)f<0&&t.updateValue(o,g),o++,d=l.next();else{a??=new Zs,r??=Sg(t,o,s,e);let _=e(o,g);if(Om(t,a,o,_))t.updateValue(o,g),o++,s++,d=l.next();else if(!r.has(_))t.attach(o,t.create(o,g)),o++,s++,d=l.next();else{let w=e(o,u);a.set(w,t.detach(o)),s--}}}for(;!d.done;)Cg(t,a,e,t.length,d.value),d=l.next()}for(;o<=s;)t.destroy(t.detach(s--));a?.forEach(l=>{t.destroy(l)})}function Om(t,n,e,i){return n!==void 0&&n.has(i)?(t.attach(e,n.get(i)),n.delete(i),!0):!1}function Cg(t,n,e,i,a){if(Om(t,n,i,e(i,a)))t.updateValue(i,a);else{let r=t.create(i,a);t.attach(i,r)}}function Sg(t,n,e,i){let a=new Set;for(let r=n;r<=e;r++)a.add(i(r,t.at(r)));return a}var Zs=class{kvMap=new Map;_vMap=void 0;has(n){return this.kvMap.has(n)}delete(n){if(!this.has(n))return!1;let e=this.kvMap.get(n);return this._vMap!==void 0&&this._vMap.has(e)?(this.kvMap.set(n,this._vMap.get(e)),this._vMap.delete(e)):this.kvMap.delete(n),!0}get(n){return this.kvMap.get(n)}set(n,e){if(this.kvMap.has(n)){let i=this.kvMap.get(n);this._vMap===void 0&&(this._vMap=new Map);let a=this._vMap;for(;a.has(i);)i=a.get(i);a.set(i,e)}else this.kvMap.set(n,e)}forEach(n){for(let[e,i]of this.kvMap)if(n(i,e),this._vMap!==void 0){let a=this._vMap;for(;a.has(i);)i=a.get(i),n(i,e)}}};function te(t,n,e,i,a,r,o,s){jn("NgControlFlow");let c=U(),l=Ce(),d=Ft(l.consts,r);return Fa(c,l,t,n,e,i,a,d,256,o,s),Iu}function Iu(t,n,e,i,a,r,o,s){return jn("NgControlFlow"),QS(t,n,e,i,a,r,o,s),Iu}function QS(t,n,e,i,a,r,o,s){let c=U(),l=Ce(),d=Ft(l.consts,r);Fa(c,l,t,n,e,i,a,d,512,o,s)}function ne(t,n){jn("NgControlFlow");let e=U(),i=Ln(),a=e[i]!==Dt?e[i]:-1,r=a!==-1?Ks(e,Le+a):void 0,o=0;if(Pt(e,i,t)){let s=j(null);try{if(r!==void 0&&Gb(r,o),t!==-1){let c=Le+t,l=Ks(e,c),d=Vm(e[W],c),u=qb(l,d,e),g=Pr(e,d,n,{dehydratedView:u});Vr(l,g,o,Ra(d,u))}}finally{j(s)}}else if(r!==void 0){let s=$b(r,o);s!==void 0&&(s[He]=n)}}var Fm=class{lContainer;$implicit;$index;constructor(n,e,i){this.lContainer=n,this.$implicit=e,this.$index=i}get $count(){return this.lContainer.length-Ue}};function yn(t,n){return n}var Lm=class{hasEmptyBlock;trackByFn;liveCollection;constructor(n,e,i){this.hasEmptyBlock=n,this.trackByFn=e,this.liveCollection=i}};function ut(t,n,e,i,a,r,o,s,c,l,d,u,g){jn("NgControlFlow");let f=U(),_=Ce(),w=c!==void 0,A=U(),ie=s?o.bind(A[gt][He]):o,fe=new Lm(w,ie);A[Le+t]=fe,Fa(f,_,t+1,n,e,i,a,Ft(_.consts,r),256),w&&Fa(f,_,t+2,c,l,d,u,Ft(_.consts,g),512)}var Pm=class extends Rm{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(n,e,i){super(),this.lContainer=n,this.hostLView=e,this.templateTNode=i}get length(){return this.lContainer.length-Ue}at(n){return this.getLView(n)[He].$implicit}attach(n,e){let i=e[Mi];this.needsIndexUpdate||=n!==this.length,Vr(this.lContainer,e,n,Ra(this.templateTNode,i)),XS(this.lContainer,n)}detach(n){return this.needsIndexUpdate||=n!==this.length-1,JS(this.lContainer,n),eE(this.lContainer,n)}create(n,e){let i=Hs(this.lContainer,this.templateTNode.tView.ssrId);return Pr(this.hostLView,this.templateTNode,new Fm(this.lContainer,e,n),{dehydratedView:i})}destroy(n){ic(n[W],n)}updateValue(n,e){this.getLView(n)[He].$implicit=e}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let n=0;n<this.length;n++)this.getLView(n)[He].$index=n}getLView(n){return tE(this.lContainer,n)}};function pt(t){let n=j(null),e=Xt();try{let i=U(),a=i[W],r=i[e],o=e+1,s=Ks(i,o);if(r.liveCollection===void 0){let l=Vm(a,o);r.liveCollection=new Pm(s,i,l)}else r.liveCollection.reset();let c=r.liveCollection;if(KS(c,t,r.trackByFn,n),c.updateIndexes(),r.hasEmptyBlock){let l=Ln(),d=c.length===0;if(Pt(i,l,d)){let u=e+2,g=Ks(i,u);if(d){let f=Vm(a,u),_=qb(g,f,i),w=Pr(i,f,void 0,{dehydratedView:_});Vr(g,w,0,Ra(f,_))}else a.firstUpdatePass&&jC(g),Gb(g,0)}}}finally{j(n)}}function Ks(t,n){return t[n]}function XS(t,n){if(t.length<=Ue)return;let e=Ue+n,i=t[e];i&&qk(i)}function JS(t,n){if(t.length<=Ue)return;let e=Ue+n,i=t[e];i&&Wk(i)}function eE(t,n){return Nr(t,n)}function tE(t,n){return $b(t,n)}function Vm(t,n){return gs(t,n)}function F(t,n,e){let i=U(),a=Ln();if(Pt(i,a,n)){let r=Ce(),o=Vi();Ib(o,i,t,n,i[De],e)}return F}function Bm(t,n,e,i,a){oc(n,t,e,a?"class":"style",i)}function p(t,n,e,i){let a=U(),r=a[W],o=t+Le,s=r.firstCreatePass?bu(o,a,2,n,uu,xs(),e,i):r.data[o];if(pn(s)){let c=a[Qt].tracingService;if(c&&c.componentCreate){let l=r.data[s.directiveStart+s.componentOffset];return c.componentCreate(Kb(l),()=>(Eg(t,n,a,s,i),p))}}return Eg(t,n,a,s,i),p}function Eg(t,n,e,i,a){if(pu(i,e,t,n,_v),Ca(i)){let r=e[W];rc(r,e,i),qm(r,i,e)}a!=null&&Lr(e,i)}function h(){let t=Ce(),n=Xe(),e=hu(n);return t.firstCreatePass&&vu(t,e),Fd(e)&&Ld(),Rd(),e.classesWithoutHost!=null&&Ow(e)&&Bm(t,e,U(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&Fw(e)&&Bm(t,e,U(),e.stylesWithoutHost,!1),h}function $(t,n,e,i){return p(t,n,e,i),h(),$}function We(t,n,e,i){let a=U(),r=a[W],o=t+Le,s=r.firstCreatePass?lS(o,r,2,n,e,i):r.data[o];return pu(s,a,t,n,_v),i!=null&&Lr(a,s),We}function qe(){let t=Xe(),n=hu(t);return Fd(n)&&Ld(),Rd(),qe}function it(t,n,e,i){return We(t,n,e,i),qe(),it}var _v=(t,n,e,i,a)=>(yr(!0),mb(n[De],i,Gd()));function Nu(t,n,e){let i=U(),a=i[W],r=t+Le,o=a.firstCreatePass?bu(r,i,8,"ng-container",uu,xs(),n,e):a.data[r];if(pu(o,i,t,"ng-container",nE),Ca(o)){let s=i[W];rc(s,i,o),qm(s,o,i)}return e!=null&&Lr(i,o),Nu}function Mu(){let t=Ce(),n=Xe(),e=hu(n);return t.firstCreatePass&&vu(t,e),Mu}function xn(t,n,e){return Nu(t,n,e),Mu(),xn}var nE=(t,n,e,i,a)=>(yr(!0),Sk(n[De],""));function ve(){return U()}function Gt(t,n,e){let i=U(),a=Ln();if(Pt(i,a,n)){let r=Ce(),o=Vi();Nb(o,i,t,n,i[De],e)}return Gt}var Br="en-US";var iE=Br;function yv(t){typeof t=="string"&&(iE=t.toLowerCase().replace(/_/g,"-"))}function C(t,n,e){let i=U(),a=Ce(),r=Xe();return xv(a,i,i[De],r,t,n,e),C}function xv(t,n,e,i,a,r,o){let s=!0,c=null;if((i.type&3||o)&&(c??=Cr(i,n,r),GC(i,t,n,o,e,a,r,c)&&(s=!1)),s){let l=i.outputs?.[a],d=i.hostDirectiveOutputs?.[a];if(d&&d.length)for(let u=0;u<d.length;u+=2){let g=d[u],f=d[u+1];c??=Cr(i,n,r),$s(i,n,g,f,a,c)}if(l&&l.length)for(let u of l)c??=Cr(i,n,r),$s(i,n,u,a,a,c)}}function x(t=1){return Ff(t)}function aE(t,n){let e=null,i=Ok(t);for(let a=0;a<n.length;a++){let r=n[a];if(r==="*"){e=a;continue}if(i===null?gb(t,r,!0):Pk(i,r))return a}return e}function he(t){let n=U()[gt][lt];if(!n.projection){let e=t?t.length:1,i=n.projection=sf(e,null),a=i.slice(),r=n.child;for(;r!==null;){if(r.type!==128){let o=t?aE(r,t):0;o!==null&&(a[o]?a[o].projectionNext=r:i[o]=r,a[o]=r)}r=r.next}}}function V(t,n=0,e,i,a,r){let o=U(),s=Ce(),c=i?t+1:null;c!==null&&Fa(o,s,c,i,a,r,null,e);let l=Wi(s,Le+t,16,null,e||null);l.projection===null&&(l.projection=n),Bd();let u=!o[Mi]||Od();o[gt][lt].projection[l.projection]===null&&c!==null?rE(o,s,c):u&&!Js(l)&&iC(s,o,l)}function rE(t,n,e){let i=Le+e,a=n.data[i],r=t[i],o=Hs(r,a.tView.ssrId),s=Pr(t,a,void 0,{dehydratedView:o});Vr(r,s,0,Ra(a,o))}function Ye(t,n,e,i){return cv(t,n,e,i),Ye}function Be(t,n,e){return sv(t,n,e),Be}function M(t){let n=U(),e=Ce(),i=Cs();_r(i+1);let a=yu(e,i);if(t.dirty&&vf(n)===((a.metadata.flags&2)===2)){if(a.matches===null)t.reset([]);else{let r=dv(n,i);t.reset(r,Wg),t.notifyOnChanges()}return!0}return!1}function T(){return _u(U(),Cs())}function dc(t,n,e,i,a){return uv(n,cv(t,e,i,a)),dc}function mc(t,n,e,i){return uv(t,sv(n,e,i)),mc}function uc(t=1){_r(Cs()+t)}function et(t){let n=jd();return Id(n,Le+t)}function Rs(t,n){return t<<17|n<<2}function Gi(t){return t>>17&32767}function oE(t){return(t&2)==2}function sE(t,n){return t&131071|n<<17}function jm(t){return t|2}function La(t){return(t&131068)>>2}function im(t,n){return t&-131069|n<<2}function cE(t){return(t&1)===1}function zm(t){return t|1}function lE(t,n,e,i,a,r){let o=r?n.classBindings:n.styleBindings,s=Gi(o),c=La(o);t[i]=e;let l=!1,d;if(Array.isArray(e)){let u=e;d=u[1],(d===null||_a(u,d)>0)&&(l=!0)}else d=e;if(a)if(c!==0){let g=Gi(t[s+1]);t[i+1]=Rs(g,s),g!==0&&(t[g+1]=im(t[g+1],i)),t[s+1]=sE(t[s+1],i)}else t[i+1]=Rs(s,0),s!==0&&(t[s+1]=im(t[s+1],i)),s=i;else t[i+1]=Rs(c,0),s===0?s=i:t[c+1]=im(t[c+1],i),c=i;l&&(t[i+1]=jm(t[i+1])),Dg(t,d,i,!0),Dg(t,d,i,!1),dE(n,d,t,i,r),o=Rs(s,c),r?n.classBindings=o:n.styleBindings=o}function dE(t,n,e,i,a){let r=a?t.residualClasses:t.residualStyles;r!=null&&typeof n=="string"&&_a(r,n)>=0&&(e[i+1]=zm(e[i+1]))}function Dg(t,n,e,i){let a=t[e+1],r=n===null,o=i?Gi(a):La(a),s=!1;for(;o!==0&&(s===!1||r);){let c=t[o],l=t[o+1];mE(c,n)&&(s=!0,t[o+1]=i?zm(l):jm(l)),o=i?Gi(l):La(l)}s&&(t[e+1]=i?jm(a):zm(a))}function mE(t,n){return t===null||n==null||(Array.isArray(t)?t[1]:t)===n?!0:Array.isArray(t)&&typeof n=="string"?_a(t,n)>=0:!1}var tn={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function uE(t){return t.substring(tn.key,tn.keyEnd)}function pE(t){return hE(t),wv(t,kv(t,0,tn.textEnd))}function wv(t,n){let e=tn.textEnd;return e===n?-1:(n=tn.keyEnd=fE(t,tn.key=n,e),kv(t,n,e))}function hE(t){tn.key=0,tn.keyEnd=0,tn.value=0,tn.valueEnd=0,tn.textEnd=t.length}function kv(t,n,e){for(;n<e&&t.charCodeAt(n)<=32;)n++;return n}function fE(t,n,e){for(;n<e&&t.charCodeAt(n)>32;)n++;return n}function oi(t,n,e){return Cv(t,n,e,!1),oi}function S(t,n){return Cv(t,n,null,!0),S}function xt(t){bE(kE,gE,t,!0)}function gE(t,n){for(let e=pE(n);e>=0;e=wv(n,e))us(t,uE(n),!0)}function Cv(t,n,e,i){let a=U(),r=Ce(),o=ws(2);if(r.firstUpdatePass&&Ev(r,t,o,i),n!==Dt&&Pt(a,o,n)){let s=r.data[Xt()];Dv(r,s,a,a[De],t,a[o+1]=SE(n,e),i,o)}}function bE(t,n,e,i){let a=Ce(),r=ws(2);a.firstUpdatePass&&Ev(a,null,r,i);let o=U();if(e!==Dt&&Pt(o,r,e)){let s=a.data[Xt()];if(Iv(s,i)&&!Sv(a,r)){let c=i?s.classesWithoutHost:s.stylesWithoutHost;c!==null&&(e=os(c,e||"")),Bm(a,s,o,e,i)}else CE(a,s,o,o[De],o[r+1],o[r+1]=wE(t,n,e),i,r)}}function Sv(t,n){return n>=t.expandoStartIndex}function Ev(t,n,e,i){let a=t.data;if(a[e+1]===null){let r=a[Xt()],o=Sv(t,e);Iv(r,i)&&n===null&&!o&&(n=!1),n=vE(a,r,n,i),lE(a,r,n,e,o,i)}}function vE(t,n,e,i){let a=Tf(t),r=i?n.residualClasses:n.residualStyles;if(a===null)(i?n.classBindings:n.styleBindings)===0&&(e=am(null,t,n,e,i),e=Tr(e,n.attrs,i),r=null);else{let o=n.directiveStylingLast;if(o===-1||t[o]!==a)if(e=am(a,t,n,e,i),r===null){let c=_E(t,n,i);c!==void 0&&Array.isArray(c)&&(c=am(null,t,n,c[1],i),c=Tr(c,n.attrs,i),yE(t,n,i,c))}else r=xE(t,n,i)}return r!==void 0&&(i?n.residualClasses=r:n.residualStyles=r),e}function _E(t,n,e){let i=e?n.classBindings:n.styleBindings;if(La(i)!==0)return t[Gi(i)]}function yE(t,n,e,i){let a=e?n.classBindings:n.styleBindings;t[Gi(a)]=i}function xE(t,n,e){let i,a=n.directiveEnd;for(let r=1+n.directiveStylingLast;r<a;r++){let o=t[r].hostAttrs;i=Tr(i,o,e)}return Tr(i,n.attrs,e)}function am(t,n,e,i,a){let r=null,o=e.directiveEnd,s=e.directiveStylingLast;for(s===-1?s=e.directiveStart:s++;s<o&&(r=n[s],i=Tr(i,r.hostAttrs,a),r!==t);)s++;return t!==null&&(e.directiveStylingLast=s),i}function Tr(t,n,e){let i=e?1:2,a=-1;if(n!==null)for(let r=0;r<n.length;r++){let o=n[r];typeof o=="number"?a=o:a===i&&(Array.isArray(t)||(t=t===void 0?[]:["",t]),us(t,o,e?!0:n[++r]))}return t===void 0?null:t}function wE(t,n,e){if(e==null||e==="")return ft;let i=[],a=$t(e);if(Array.isArray(a))for(let r=0;r<a.length;r++)t(i,a[r],!0);else if(a instanceof Set)for(let r of a)t(i,r,!0);else if(typeof a=="object")for(let r in a)Object.hasOwn(a,r)&&t(i,r,a[r]);else typeof a=="string"&&n(i,a);return i}function kE(t,n,e){let i=String(n);i!==""&&!i.includes(" ")&&us(t,i,e)}function CE(t,n,e,i,a,r,o,s){a===Dt&&(a=ft);let c=0,l=0,d=0<a.length?a[0]:null,u=0<r.length?r[0]:null;for(;d!==null||u!==null;){let g=c<a.length?a[c+1]:void 0,f=l<r.length?r[l+1]:void 0,_=null,w;d===u?(c+=2,l+=2,g!==f&&(_=u,w=f)):u===null||d!==null&&d<u?(c+=2,_=d):(l+=2,_=u,w=f),_!==null&&Dv(t,n,e,i,_,w,o,s),d=c<a.length?a[c]:null,u=l<r.length?r[l]:null}}function Dv(t,n,e,i,a,r,o,s){if(!(n.type&3))return;let c=t.data,l=c[s+1],d=cE(l)?Ig(c,n,e,a,La(l),o):void 0;if(!Qs(d)){Qs(r)||oE(l)&&(r=Ig(c,null,e,a,s,o));let u=Dd(Xt(),e);rC(i,o,u,a,r)}}function Ig(t,n,e,i,a,r){let o=n===null,s;for(;a>0;){let c=t[a],l=Array.isArray(c),d=l?c[1]:c,u=d===null,g=e[a+1];g===Dt&&(g=u?ft:void 0);let f=u?ps(g,i):d===i?g:void 0;if(l&&!Qs(f)&&(f=ps(c,i)),Qs(f)&&(s=f,o))return s;let _=t[a+1];a=o?Gi(_):La(_)}if(n!==null){let c=r?n.residualClasses:n.residualStyles;c!=null&&(s=ps(c,i))}return s}function Qs(t){return t!==void 0}function SE(t,n){return t==null||t===""||(typeof n=="string"?t=$t(t)+n:typeof t=="object"&&(t=rs($t(t)))),t}function Iv(t,n){return(t.flags&(n?8:16))!==0}function b(t,n=""){let e=U(),i=Ce(),a=t+Le,r=i.firstCreatePass?Wi(i,a,1,n,null):i.data[a],o=EE(i,e,r,n);e[a]=o,Ds()&&su(i,e,o,r),Pi(r,!1)}var EE=(t,n,e,i)=>(yr(!0),kk(n[De],i));function Nv(t,n,e,i=""){return Pt(t,Ln(),e)?n+ba(e)+i:Dt}function DE(t,n,e,i,a,r=""){let o=Ef(),s=Xb(t,o,e,a);return ws(2),s?n+ba(e)+i+ba(a)+r:Dt}function de(t){return zn("",t),de}function zn(t,n,e){let i=U(),a=Nv(i,t,n,e);return a!==Dt&&Mv(i,Xt(),a),zn}function si(t,n,e,i,a){let r=U(),o=DE(r,t,n,e,i,a);return o!==Dt&&Mv(r,Xt(),o),si}function Mv(t,n,e){let i=Dd(n,t);Ck(t[De],i,e)}function wn(t,n,e){Ms(n)&&(n=n());let i=U(),a=Ln();if(Pt(i,a,n)){let r=Ce(),o=Vi();Ib(o,i,t,n,i[De],e)}return wn}function Hn(t,n){let e=Ms(t);return e&&t.set(n),e}function kn(t,n){let e=U(),i=Ce(),a=Xe();return xv(i,e,e[De],a,t,n),kn}var Tv={};function pc(t){jn("NgLet");let n=Ce(),e=U(),i=t+Le,a=Wi(n,i,128,null,null);return Pi(a,!1),bs(n,e,i,Tv),pc}function Tu(t){let n=Ce(),e=U(),i=Xt();return bs(n,e,i,t),t}function Au(t){let n=jd(),e=Id(n,Le+t);if(e===Tv)throw new H(314,!1);return e}function Ru(t,n,e=""){return Nv(U(),t,n,e)}function Ng(t,n,e){let i=Ce();i.firstCreatePass&&Av(n,i.data,i.blueprint,hn(t),e)}function Av(t,n,e,i,a){if(t=st(t),Array.isArray(t))for(let r=0;r<t.length;r++)Av(t[r],n,e,i,a);else{let r=Ce(),o=U(),s=Xe(),c=Si(t)?t:st(t.provide),l=xd(t),d=s.providerIndexes&1048575,u=s.directiveStart,g=s.providerIndexes>>20;if(Si(t)||!t.multi){let f=new Ui(l,a,Ae,null),_=om(c,n,a?d:d+g,u);_===-1?(lm(zs(s,o),r,c),rm(r,t,n.length),n.push(c),s.directiveStart++,s.directiveEnd++,a&&(s.providerIndexes+=1048576),e.push(f),o.push(f)):(e[_]=f,o[_]=f)}else{let f=om(c,n,d+g,u),_=om(c,n,d,d+g),w=f>=0&&e[f],A=_>=0&&e[_];if(a&&!A||!a&&!w){lm(zs(s,o),r,c);let ie=ME(a?NE:IE,e.length,a,i,l,t);!a&&A&&(e[_].providerFactory=ie),rm(r,t,n.length,0),n.push(c),s.directiveStart++,s.directiveEnd++,a&&(s.providerIndexes+=1048576),e.push(ie),o.push(ie)}else{let ie=Rv(e[a?_:f],l,!a&&i);rm(r,t,f>-1?f:_,ie)}!a&&i&&A&&e[_].componentProviders++}}}function rm(t,n,e,i){let a=Si(n),r=pf(n);if(a||r){let c=(r?st(n.useClass):n).prototype.ngOnDestroy;if(c){let l=t.destroyHooks||(t.destroyHooks=[]);if(!a&&n.multi){let d=l.indexOf(e);d===-1?l.push(e,[i,c]):l[d+1].push(i,c)}else l.push(e,c)}}}function Rv(t,n,e){return e&&t.componentProviders++,t.multi.push(n)-1}function om(t,n,e,i){for(let a=e;a<i;a++)if(n[a]===t)return a;return-1}function IE(t,n,e,i,a){return Hm(this.multi,[])}function NE(t,n,e,i,a){let r=this.multi,o;if(this.providerFactory){let s=this.providerFactory.componentProviders,c=Er(i,i[W],this.providerFactory.index,a);o=c.slice(0,s),Hm(r,o);for(let l=s;l<c.length;l++)o.push(c[l])}else o=[],Hm(r,o);return o}function Hm(t,n){for(let e=0;e<t.length;e++){let i=t[e];n.push(i())}return n}function ME(t,n,e,i,a,r){let o=new Ui(t,e,Ae,null);return o.multi=[],o.index=n,o.componentProviders=0,Rv(o,a,i&&!e),o}function _e(t,n){return e=>{e.providersResolver=(i,a)=>Ng(i,a?a(t):t,!1),n&&(e.viewProvidersResolver=(i,a)=>Ng(i,a?a(n):n,!0))}}function hc(t,n,e){return TE(U(),Hd(),t,n,e)}function Ou(t,n,e,i){return AE(U(),Hd(),t,n,e,i)}function Ov(t,n){let e=t[n];return e===Dt?void 0:e}function TE(t,n,e,i,a,r){let o=n+e;return Pt(t,o,a)?Qb(t,o+1,r?i.call(r,a):i(a)):Ov(t,o+1)}function AE(t,n,e,i,a,r,o){let s=n+e;return Xb(t,s,a,r)?Qb(t,s+2,o?i.call(o,a,r):i(a,r)):Ov(t,s+2)}function ci(t,n){return sc(t,n)}var Fv=(()=>{class t{applicationErrorHandler=m(ni);appRef=m(rn);taskService=m(Ea);ngZone=m(N);zonelessEnabled=m(wr);tracing=m(bn,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new pe;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(dr):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(m(Kd,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:case 2:{this.appRef.dirtyFlags|=2;break}case 3:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?jf:Wd;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(dr+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})}return t})();function Lv(){return[{provide:Rn,useExisting:Fv},{provide:N,useClass:mr},{provide:wr,useValue:!0}]}function RE(){return typeof $localize<"u"&&$localize.locale||Br}var fc=new y("",{factory:()=>m(fc,{optional:!0,skipSelf:!0})||RE()});function at(t,n){return tr(t,n?.equal)}function tt(t){return xh(t)}var Pv=class t extends Error{_brand;constructor(n){super(n)}static IDLE=new t("IDLE");static LOADING=new t("LOADING")};function Hv(t,n){let e=Object.create(Mg);e.value=t,e.transformFn=n?.transform;function i(){if(qn(e),e.value===Xs){let a=null;throw new H(-950,a)}return e.value}return i[Ke]=e,i}var Un=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>$m(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}},Uv=(()=>{let t=new y("");return t.__NG_ELEMENT_ID__=n=>{let e=Xe();if(e===null)throw new H(-204,!1);if(e.type&2)return e.value;if(n&8)return null;throw new H(-204,!1)},t})();function Vv(t,n){return Hv(t,n)}function XE(t){return Hv(Xs,t)}var Vu=(Vv.required=XE,Vv);function Bv(t,n){return xu(n)}function JE(t,n){return wu(n)}var zr=(Bv.required=JE,Bv);function jv(t,n){return xu(n)}function eD(t,n){return wu(n)}var $v=(jv.required=eD,jv);var tD=1e4;var d8=tD-1e3;var Ie=(()=>{class t{static __NG_ELEMENT_ID__=nD}return t})();function nD(t){return iD(Xe(),U(),(t&16)===16)}function iD(t,n,e){if(pn(t)&&!e){let i=Ut(t.index,n);return new ii(i,i)}else if(t.type&175){let i=n[gt];return new ii(i,n)}return null}var Lu=new y(""),aD=new y("");function jr(t){return!t.moduleRef}function rD(t){let n=jr(t)?t.r3Injector:t.moduleRef.injector,e=n.get(N);return e.run(()=>{jr(t)?t.r3Injector.resolveInjectorInitializers():t.moduleRef.resolveInjectorInitializers();let i=n.get(ni),a;if(e.runOutsideAngular(()=>{a=e.onError.subscribe({next:i})}),jr(t)){let r=()=>n.destroy(),o=t.platformInjector.get(Lu);o.add(r),n.onDestroy(()=>{a.unsubscribe(),o.delete(r)})}else{let r=()=>t.moduleRef.destroy(),o=t.platformInjector.get(Lu);o.add(r),t.moduleRef.onDestroy(()=>{Sr(t.allPlatformModules,t.moduleRef),a.unsubscribe(),o.delete(r)})}return sD(i,e,()=>{let r=n.get(Ea),o=r.add(),s=n.get(Cu);return s.runInitializers(),s.donePromise.then(()=>{let c=n.get(fc,Br);if(yv(c||Br),!n.get(aD,!0))return jr(t)?n.get(rn):(t.allPlatformModules.push(t.moduleRef),t.moduleRef);if(jr(t)){let d=n.get(rn);return t.rootComponent!==void 0&&d.bootstrap(t.rootComponent),d}else return oD?.(t.moduleRef,t.allPlatformModules),t.moduleRef}).finally(()=>{r.remove(o)})})})}var oD;function sD(t,n,e){try{let i=e();return Yi(i)?i.catch(a=>{throw n.runOutsideAngular(()=>t(a)),a}):i}catch(i){throw n.runOutsideAngular(()=>t(i)),i}}var gc=null;function cD(t=[],n){return X.create({name:n,providers:[{provide:fr,useValue:"platform"},{provide:Lu,useValue:new Set([()=>gc=null])},...t]})}function lD(t=[]){if(gc)return gc;let n=cD(t);return gc=n,vv(),dD(n),n}function dD(t){let n=t.get(Is,null);hs(t,()=>{n?.forEach(e=>e())})}function Gv(t){let{rootComponent:n,appProviders:e,platformProviders:i,platformRef:a}=t;ke(be.BootstrapApplicationStart);try{let r=a?.injector??lD(i),o=[Lv(),Uf,...e||[]],s=new Mr({providers:o,parent:r,debugName:"",runEnvironmentInitializers:!1});return rD({r3Injector:s.injector,platformInjector:r,rootComponent:n})}catch(r){return Promise.reject(r)}finally{ke(be.BootstrapApplicationEnd)}}function I(t){return typeof t=="boolean"?t:t!=null&&t!=="false"}function ht(t,n=NaN){return!isNaN(parseFloat(t))&&!isNaN(Number(t))?Number(t):n}var Fu=Symbol("NOT_SET"),Wv=new Set,mD=ge(P({},oa),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:Fu,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(t){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==Fu&&!aa(this))return this.signal;try{for(let a of this.cleanup??Wv)a()}finally{this.cleanup?.clear()}let n=[];t!==void 0&&n.push(t),n.push(this.registerCleanupFn);let e=Yn(this),i;try{i=this.userFn.apply(null,n)}finally{fi(this,e)}return(this.value===Fu||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),Pu=class extends Dr{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,e,i,a,r,o=null,s){super(n,[void 0,void 0,void 0,void 0],i,!1,r.get(Et),o),this.scheduler=a;for(let c of au){let l=e[c];if(l===void 0)continue;let d=Object.create(mD);d.sequence=this,d.phase=c,d.userFn=l,d.dirty=!0,d.signal=()=>(qn(d),d.value),d.signal[Ke]=d,d.registerCleanupFn=u=>(d.cleanup??=new Set).add(u),this.nodes[c]=d,this.hooks[c]=u=>d.phaseFn(u)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let e of n.cleanup??Wv)e()}finally{Zn(n)}}};function Bu(t,n){let e=n?.injector??m(X),i=e.get(Rn),a=e.get(nc),r=e.get(bn,null,{optional:!0});a.impl??=e.get(ru);let o=t;typeof o=="function"&&(o={mixedReadWrite:t});let s=e.get(Da,null,{optional:!0}),c=new Pu(a.impl,[o.earlyRead,o.write,o.mixedReadWrite,o.read],s?.view,i,e,r?.snapshot(null),void 0);return a.impl.register(c),c}function bc(t,n){let e=Qn(t),i=n.elementInjector||ya(),r=new Oa(e).create(i,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings);return n.onError&&(r.hostView._lView[ka]=n.onError),r}var qv=null;function Wt(){return qv}function ju(t){qv??=t}var Hr=class{},vc=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=Te({token:t,factory:()=>m(Yv),providedIn:"platform"})}return t})();var Yv=(()=>{class t extends vc{_location;_history;_doc=m(ee);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return Wt().getBaseHref(this._doc)}onPopState(e){let i=Wt().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",e,!1),()=>i.removeEventListener("popstate",e)}onHashChange(e){let i=Wt().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",e,!1),()=>i.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,i,a){this._history.pushState(e,i,a)}replaceState(e,i,a){this._history.replaceState(e,i,a)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||t)};static \u0275prov=Te({token:t,factory:()=>new t,providedIn:"platform"})}return t})();function Qv(t,n){return t?n?t.endsWith("/")?n.startsWith("/")?t+n.slice(1):t+n:n.startsWith("/")?t+n:`${t}/${n}`:t:n}function Zv(t){let n=t.search(/#|\?|$/);return t[n-1]==="/"?t.slice(0,n-1)+t.slice(n):t}function li(t){return t&&t[0]!=="?"?`?${t}`:t}var _c=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=Te({token:t,factory:()=>m(pD),providedIn:"root"})}return t})(),uD=new y(""),pD=(()=>{class t extends _c{_platformLocation;_baseHref;_removeListenerFns=[];constructor(e,i){super(),this._platformLocation=e,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??m(ee).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(e){this._removeListenerFns.push(this._platformLocation.onPopState(e),this._platformLocation.onHashChange(e))}getBaseHref(){return this._baseHref}prepareExternalUrl(e){return Qv(this._baseHref,e)}path(e=!1){let i=this._platformLocation.pathname+li(this._platformLocation.search),a=this._platformLocation.hash;return a&&e?`${i}${a}`:i}pushState(e,i,a,r){let o=this.prepareExternalUrl(a+li(r));this._platformLocation.pushState(e,i,o)}replaceState(e,i,a,r){let o=this.prepareExternalUrl(a+li(r));this._platformLocation.replaceState(e,i,o)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(e=0){this._platformLocation.historyGo?.(e)}static \u0275fac=function(i){return new(i||t)(ce(vc),ce(uD,8))};static \u0275prov=Te({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var yc=(()=>{class t{_subject=new E;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(e){this._locationStrategy=e;let i=this._locationStrategy.getBaseHref();this._basePath=gD(Zv(Kv(i))),this._locationStrategy.onPopState(a=>{let r={url:this.path(!0),pop:!0,state:a.state,type:a.type};a.hasUAVisualTransition&&(r.hasUAVisualTransition=!0),this._subject.next(r)})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(e=!1){return this.normalize(this._locationStrategy.path(e))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(e,i=""){return this.path()==this.normalize(e+li(i))}normalize(e){return t.stripTrailingSlash(fD(this._basePath,Kv(e)))}prepareExternalUrl(e){return e&&e[0]!=="/"&&(e="/"+e),this._locationStrategy.prepareExternalUrl(e)}go(e,i="",a=null){this._locationStrategy.pushState(a,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+li(i)),a)}replaceState(e,i="",a=null){this._locationStrategy.replaceState(a,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+li(i)),a)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(e=0){this._locationStrategy.historyGo?.(e)}onUrlChange(e){return this._urlChangeListeners.push(e),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(e);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(e="",i){this._urlChangeListeners.forEach(a=>a(e,i))}subscribe(e,i,a){return this._subject.subscribe({next:e,error:i??void 0,complete:a??void 0})}static normalizeQueryParams=li;static joinWithSlash=Qv;static stripTrailingSlash=Zv;static \u0275fac=function(i){return new(i||t)(ce(_c))};static \u0275prov=Te({token:t,factory:()=>hD(),providedIn:"root"})}return t})();function hD(){return new yc(ce(_c))}function fD(t,n){if(!t||!n.startsWith(t))return n;let e=n.substring(t.length);return e===""||["/",";","?","#"].includes(e[0])?e:n}function Kv(t){return t.replace(/\/index\.html$/,"")}function gD(t){if(new RegExp("^(https?:)?//").test(t)){let[,e]=t.split(/\/\/[^\/]+/);return e}return t}var Ur=(()=>{class t{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=m(X);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let a=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,a,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,i,a)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,a):!1,get:(e,i,a)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,a)}})}static \u0275fac=function(i){return new(i||t)(Ae(Vt))};static \u0275dir=D({type:t,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[rt]})}return t})();function zu(t,n){n=encodeURIComponent(n);for(let e of t.split(";")){let i=e.indexOf("="),[a,r]=i==-1?[e,""]:[e.slice(0,i),e.slice(i+1)];if(a.trim()!==n)continue;let o=r;try{o=decodeURIComponent(r)}catch{}return o.length>1&&o[0]==='"'&&o[o.length-1]==='"'&&(o=o.slice(1,-1)),o}return null}var Hu="browser";function Xv(t){return t===Hu}var $r=class{_doc;constructor(n){this._doc=n}manager},xc=(()=>{class t extends $r{constructor(e){super(e)}supports(e){return!0}addEventListener(e,i,a,r){return e.addEventListener(i,a,r),()=>this.removeEventListener(e,i,a,r)}removeEventListener(e,i,a,r){return e.removeEventListener(i,a,r)}static \u0275fac=function(i){return new(i||t)(ce(ee))};static \u0275prov=Te({token:t,factory:t.\u0275fac})}return t})(),Cc=new y(""),Wu=(()=>{class t{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,i){this._zone=i,e.forEach(o=>{o.manager=this});let a=e.filter(o=>!(o instanceof xc));this._plugins=a.slice().reverse();let r=e.find(o=>o instanceof xc);r&&this._plugins.push(r)}addEventListener(e,i,a,r){return this._findPluginFor(i).addEventListener(e,i,a,r)}getZone(){return this._zone}_findPluginFor(e){let i=this._eventNameToPlugin.get(e);if(i)return i;if(i=this._plugins.find(r=>r.supports(e)),!i)throw new H(-5101,!1);return this._eventNameToPlugin.set(e,i),i}static \u0275fac=function(i){return new(i||t)(ce(Cc),ce(N))};static \u0275prov=Te({token:t,factory:t.\u0275fac})}return t})(),Uu="ng-app-id";function Jv(t){for(let n of t)n.remove()}function e_(t,n){let e=n.createElement("style");return e.textContent=t,e}function yD(t,n,e,i){let a=t.head?.querySelectorAll(`style[${Uu}="${n}"],link[${Uu}="${n}"]`);if(!a||a.length===0)return!1;for(let r of a)r.removeAttribute(Uu),r instanceof HTMLLinkElement?i.set(r.href.slice(r.href.lastIndexOf("/")+1),{usage:0,elements:[r]}):r.textContent&&e.set(r.textContent,{usage:0,elements:[r]});return!0}function Gu(t,n){let e=n.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",t),e}var qu=(()=>{class t{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,i,a,r={}){this.doc=e,this.appId=i,this.nonce=a,yD(e,i,this.inline,this.external)&&this.hosts.add(e.head)}addStyles(e,i){for(let a of e)this.addUsage(a,this.inline,e_);i?.forEach(a=>this.addUsage(a,this.external,Gu))}removeStyles(e,i){for(let a of e)this.removeUsage(a,this.inline);i?.forEach(a=>this.removeUsage(a,this.external))}addUsage(e,i,a){let r=i.get(e);r?r.usage++:i.set(e,{usage:1,elements:[...this.hosts].map(o=>this.addElement(o,a(e,this.doc)))})}removeUsage(e,i){let a=i.get(e);a&&(a.usage--,a.usage<=0&&(Jv(a.elements),i.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])Jv(e);this.hosts.clear()}addHost(e){if(!this.hosts.has(e)){this.hosts.add(e);for(let[i,{elements:a}]of this.inline)a.push(this.addElement(e,e_(i,this.doc)));for(let[i,{elements:a}]of this.external)a.push(this.addElement(e,Gu(i,this.doc)))}}removeHost(e){this.hosts.delete(e);for(let i of[...this.inline.values(),...this.external.values()]){let a=[];for(let r of i.elements)r.parentNode===e?r.remove():a.push(r);i.elements=a}}addElement(e,i){return this.nonce&&i.setAttribute("nonce",this.nonce),e.appendChild(i)}static \u0275fac=function(i){return new(i||t)(ce(ee),ce(Bi),ce(zi,8),ce(ji))};static \u0275prov=Te({token:t,factory:t.\u0275fac})}return t})(),$u={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},Yu=/%COMP%/g;var n_="%COMP%",xD=`_nghost-${n_}`,wD=`_ngcontent-${n_}`,kD=!0,CD=new y("",{factory:()=>kD}),SD=new y("");function ED(t){return wD.replace(Yu,t)}function DD(t){return xD.replace(Yu,t)}function i_(t,n){return n.map(e=>e.replace(Yu,t))}var Zu=(()=>{class t{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;cssVarNamespace;constructor(e,i,a,r,o,s,c=null,l=null,d=null){this.eventManager=e,this.sharedStylesHost=i,this.appId=a,this.removeStylesOnCompDestroy=r,this.doc=o,this.ngZone=s,this.nonce=c,this.tracingService=l,this.cssVarNamespace=d??"",this.defaultRenderer=new Gr(e,o,s,this.tracingService,this.cssVarNamespace)}createRenderer(e,i){if(!e||!i)return this.defaultRenderer;let a=this.getOrCreateRenderer(e,i);return a instanceof kc?a.applyToHost(e):a instanceof Wr&&a.applyStyles(),a}getOrCreateRenderer(e,i){let a=this.rendererByCompId,r=a.get(i.id);if(!r){let o=this.doc,s=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,d=this.removeStylesOnCompDestroy,u=this.tracingService;switch(i.encapsulation){case an.Emulated:r=new kc(c,l,i,this.appId,d,o,s,u,this.cssVarNamespace);break;case an.ShadowDom:return new wc(c,e,i,o,s,this.nonce,u,this.cssVarNamespace,l);case an.ExperimentalIsolatedShadowDom:return new wc(c,e,i,o,s,this.nonce,u,this.cssVarNamespace);default:r=new Wr(c,l,i,d,o,s,u,this.cssVarNamespace);break}a.set(i.id,r)}return r}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(i){return new(i||t)(ce(Wu),ce(qi),ce(Bi),ce(CD),ce(ee),ce(N),ce(zi),ce(bn,8),ce(SD,8))};static \u0275prov=Te({token:t,factory:t.\u0275fac})}return t})(),Gr=class{eventManager;doc;ngZone;tracingService;cssVarNamespace;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,e,i,a,r=""){this.eventManager=n,this.doc=e,this.ngZone=i,this.tracingService=a,this.cssVarNamespace=r}destroy(){}destroyNode=null;createElement(n,e){return e?this.doc.createElementNS($u[e]||e,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,e){(t_(n)?n.content:n).appendChild(e)}insertBefore(n,e,i){if(n){let a=t_(n)?n.content:n;if(i!=null&&i.parentNode!==a)throw new H(-5106,!1);a.insertBefore(e,i)}}removeChild(n,e){e.remove()}selectRootElement(n,e){let i=typeof n=="string"?this.doc.querySelector(n):n;if(!i)throw new H(-5104,!1);return e||(i.textContent=""),i}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,e,i,a){if(a){e=a+":"+e;let r=$u[a];r?n.setAttributeNS(r,e,i):n.setAttribute(e,i)}else n.setAttribute(e,i)}removeAttribute(n,e,i){if(i){let a=$u[i];a?n.removeAttributeNS(a,e):n.removeAttribute(`${i}:${e}`)}else n.removeAttribute(e)}addClass(n,e){n.classList.add(e)}removeClass(n,e){n.classList.remove(e)}setStyle(n,e,i,a){let r=e.startsWith("--");r&&(e=e.replace("%NS%",this.cssVarNamespace)),r||a&(gn.DashCase|gn.Important)?n.style.setProperty(e,i,a&gn.Important?"important":""):n.style[e]=i}removeStyle(n,e,i){let a=e.startsWith("--");a&&(e=e.replace("%NS%",this.cssVarNamespace)),a||i&gn.DashCase?n.style.removeProperty(e):n.style[e]=""}setProperty(n,e,i){n!=null&&(n[e]=i)}setValue(n,e){n.nodeValue=e}listen(n,e,i,a){if(typeof n=="string"&&(n=Wt().getGlobalEventTarget(this.doc,n),!n))throw new H(-5102,!1);let r=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(r=this.tracingService.wrapEventListener(n,e,r)),this.eventManager.addEventListener(n,e,r,a)}decoratePreventDefault(n){return e=>{if(e==="__ngUnwrap__")return n;n(e)===!1&&e.preventDefault()}}};function t_(t){return t.tagName==="TEMPLATE"&&t.content!==void 0}var wc=class extends Gr{hostEl;sharedStylesHost;shadowRoot;constructor(n,e,i,a,r,o,s,c,l){super(n,a,r,s,c),this.hostEl=e,this.sharedStylesHost=l,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let d=i.styles;d=i_(i.id,d).map(g=>g.replace(/%NS%/g,c));for(let g of d){let f=document.createElement("style");o&&f.setAttribute("nonce",o),f.textContent=g,this.shadowRoot.appendChild(f)}let u=i.getExternalStyles?.();if(u)for(let g of u){let f=Gu(g,a);o&&f.setAttribute("nonce",o),this.shadowRoot.appendChild(f)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,e){return super.appendChild(this.nodeOrShadowRoot(n),e)}insertBefore(n,e,i){return super.insertBefore(this.nodeOrShadowRoot(n),e,i)}removeChild(n,e){return super.removeChild(null,e)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},Wr=class extends Gr{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,e,i,a,r,o,s,c,l){super(n,r,o,s,c),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=a;let d=i.styles,u=l?i_(l,d):d;this.styles=u.map(g=>g.replace(/%NS%/g,c)),this.styleUrls=i.getExternalStyles?.(l)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&iu.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},kc=class extends Wr{contentAttr;hostAttr;constructor(n,e,i,a,r,o,s,c,l){let d=a+"-"+i.id;super(n,e,i,r,o,s,c,l,d),this.contentAttr=ED(d),this.hostAttr=DD(d)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,e){let i=super.createElement(n,e);return super.setAttribute(i,this.contentAttr,""),i}};var Sc=class t extends Hr{supportsDOMEvents=!0;static makeCurrent(){ju(new t)}onAndCancel(n,e,i,a){return n.addEventListener(e,i,a),()=>{n.removeEventListener(e,i,a)}}dispatchEvent(n,e){n.dispatchEvent(e)}remove(n){n.remove()}createElement(n,e){return e=e||this.getDefaultDocument(),e.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,e){return e==="window"?window:e==="document"?n:e==="body"?n.body:null}getBaseHref(n){let e=ID();return e==null?null:ND(e)}resetBaseElement(){qr=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return zu(document.cookie,n)}},qr=null;function ID(){return qr=qr||document.head.querySelector("base"),qr?qr.getAttribute("href"):null}function ND(t){return new URL(t,document.baseURI).pathname}var a_=["alt","control","meta","shift"],MD={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},TD={alt:t=>t.altKey,control:t=>t.ctrlKey,meta:t=>t.metaKey,shift:t=>t.shiftKey},r_=(()=>{class t extends $r{constructor(e){super(e)}supports(e){return t.parseEventName(e)!=null}addEventListener(e,i,a,r){let o=t.parseEventName(i),s=t.eventCallback(o.fullKey,a,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>Wt().onAndCancel(e,o.domEventName,s,r))}static parseEventName(e){let i=e.toLowerCase().split("."),a=i.shift();if(i.length===0||!(a==="keydown"||a==="keyup"))return null;let r=t._normalizeKey(i.pop()),o="",s=i.indexOf("code");if(s>-1&&(i.splice(s,1),o="code."),a_.forEach(l=>{let d=i.indexOf(l);d>-1&&(i.splice(d,1),o+=l+".")}),o+=r,i.length!=0||r.length===0)return null;let c={};return c.domEventName=a,c.fullKey=o,c}static matchEventFullKeyCode(e,i){let a=MD[e.key]||e.key,r="";return i.indexOf("code.")>-1&&(a=e.code,r="code."),a==null||!a?!1:(a=a.toLowerCase(),a===" "?a="space":a==="."&&(a="dot"),a_.forEach(o=>{if(o!==a){let s=TD[o];s(e)&&(r+=o+".")}}),r+=a,r===i)}static eventCallback(e,i,a){return r=>{t.matchEventFullKeyCode(r,e)&&a.runGuarded(()=>i(r))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(i){return new(i||t)(ce(ee))};static \u0275prov=Te({token:t,factory:t.\u0275fac})}return t})();async function Ku(t,n,e){let i=P({rootComponent:t},AD(n,e));return Gv(i)}function AD(t,n){return{platformRef:n?.platformRef,appProviders:[...PD,...t?.providers??[]],platformProviders:LD}}function RD(){Sc.makeCurrent()}function OD(){return new mn}function FD(){return Wm(document),document}var LD=[{provide:ji,useValue:Hu},{provide:Is,useValue:RD,multi:!0},{provide:ee,useFactory:FD}];var PD=[{provide:fr,useValue:"root"},{provide:mn,useFactory:OD},{provide:Cc,useClass:xc,multi:!0},{provide:Cc,useClass:r_,multi:!0},Zu,{provide:qi,useClass:qu},{provide:qu,useExisting:qi},Wu,{provide:Je,useExisting:Zu},[]];var Qu=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=Te({token:t,factory:function(i){let a=null;return i?a=new(i||t):a=ce(VD),a},providedIn:"root"})}return t})(),VD=(()=>{class t extends Qu{_doc=m(ee);sanitize(e,i){if(i==null)return null;switch(e){case Rt.NONE:return i;case Rt.HTML:return ri(i,"HTML")?$t(i):Jm(this._doc,String(i)).toString();case Rt.STYLE:return ri(i,"Style")?$t(i):i;case Rt.SCRIPT:if(ri(i,"Script"))return $t(i);throw new H(5200,!1);case Rt.URL:return ri(i,"URL")?$t(i):Rr(String(i));case Rt.RESOURCE_URL:if(ri(i,"ResourceURL"))return $t(i);throw new H(-5201,!1);default:throw new H(5202,!1)}}bypassSecurityTrustHtml(e){return Ym(e)}bypassSecurityTrustStyle(e){return Zm(e)}bypassSecurityTrustScript(e){return Km(e)}bypassSecurityTrustUrl(e){return Qm(e)}bypassSecurityTrustResourceUrl(e){return Xm(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})}return t})();var o_={providers:[Zd()]};var f_=(()=>{class t{_renderer;_elementRef;onChange=e=>{};onTouched=()=>{};constructor(e,i){this._renderer=e,this._elementRef=i}setProperty(e,i){this._renderer.setProperty(this._elementRef.nativeElement,e,i)}registerOnTouched(e){this.onTouched=e}registerOnChange(e){this.onChange=e}setDisabledState(e){this.setProperty("disabled",e)}static \u0275fac=function(i){return new(i||t)(Ae(Se),Ae(L))};static \u0275dir=D({type:t})}return t})(),BD=(()=>{class t extends f_{static \u0275fac=(()=>{let e;return function(a){return(e||(e=Fe(t)))(a||t)}})();static \u0275dir=D({type:t,features:[le]})}return t})(),Cn=new y("");var jD={provide:Cn,useExisting:ze(()=>Lc),multi:!0};function zD(){let t=Wt()?Wt().getUserAgent():"";return/android (\d+)/.test(t.toLowerCase())}var HD=new y(""),Lc=(()=>{class t extends f_{_compositionMode;_composing=!1;constructor(e,i,a){super(e,i),this._compositionMode=a,this._compositionMode==null&&(this._compositionMode=!zD())}writeValue(e){let i=e??"";this.setProperty("value",i)}_handleInput(e){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(e)}_compositionStart(){this._composing=!0}_compositionEnd(e){this._composing=!1,this._compositionMode&&this.onChange(e)}static \u0275fac=function(i){return new(i||t)(Ae(Se),Ae(L),Ae(HD,8))};static \u0275dir=D({type:t,selectors:[["input","formControlName","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControlName","",3,"ngNoCva",""],["input","formControl","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControl","",3,"ngNoCva",""],["input","ngModel","",3,"type","checkbox",3,"ngNoCva",""],["textarea","ngModel","",3,"ngNoCva",""],["","ngDefaultControl",""]],hostBindings:function(i,a){i&1&&C("input",function(o){return a._handleInput(o.target.value)})("blur",function(){return a.onTouched()})("compositionstart",function(){return a._compositionStart()})("compositionend",function(o){return a._compositionEnd(o.target.value)})},standalone:!1,features:[_e([jD]),le]})}return t})();function tp(t){return t==null||np(t)===0}function np(t){return t==null?null:Array.isArray(t)||typeof t=="string"?t.length:t instanceof Set?t.size:null}var Ki=new y(""),ip=new y(""),UD=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,ja=class{static min(n){return $D(n)}static max(n){return GD(n)}static required(n){return g_(n)}static requiredTrue(n){return WD(n)}static email(n){return qD(n)}static minLength(n){return YD(n)}static maxLength(n){return b_(n)}static pattern(n){return ZD(n)}static nullValidator(n){return Dc()}static compose(n){return k_(n)}static composeAsync(n){return C_(n)}};function $D(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e<t?{min:{min:t,actual:n.value}}:null}}function GD(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e>t?{max:{max:t,actual:n.value}}:null}}function g_(t){return tp(t.value)?{required:!0}:null}function WD(t){return t.value===!0?null:{required:!0}}function qD(t){return tp(t.value)||UD.test(t.value)?null:{email:!0}}function YD(t){return n=>{let e=n.value?.length??np(n.value);return e===null||e===0?null:e<t?{minlength:{requiredLength:t,actualLength:e}}:null}}function b_(t){return n=>{let e=n.value?.length??np(n.value);return e!==null&&e>t?{maxlength:{requiredLength:t,actualLength:e}}:null}}function ZD(t){if(!t)return Dc;let n,e;return typeof t=="string"?(e="",t.charAt(0)!=="^"&&(e+="^"),e+=t,t.charAt(t.length-1)!=="$"&&(e+="$"),n=new RegExp(e)):(e=t.toString(),n=t),i=>{if(tp(i.value))return null;let a=i.value;return n.test(a)?null:{pattern:{requiredPattern:e,actualValue:a}}}}function Dc(t){return null}function v_(t){return t!=null}function __(t){return Yi(t)?Bt(t):t}function y_(t){let n={};return t.forEach(e=>{n=e!=null?P(P({},n),e):n}),Object.keys(n).length===0?null:n}function x_(t,n){return n.map(e=>e(t))}function KD(t){return!t.validate}function w_(t){return t.map(n=>KD(n)?n:e=>n.validate(e))}function k_(t){if(!t)return null;let n=t.filter(v_);return n.length==0?null:function(e){return y_(x_(e,n))}}function ap(t){return t!=null?k_(w_(t)):null}function C_(t){if(!t)return null;let n=t.filter(v_);return n.length==0?null:function(e){let i=x_(e,n).map(__);return Wl(i).pipe(Qe(y_))}}function rp(t){return t!=null?C_(w_(t)):null}function s_(t,n){return t===null?[n]:Array.isArray(t)?[...t,n]:[t,n]}function S_(t){return t._rawValidators}function E_(t){return t._rawAsyncValidators}function Xu(t){return t?Array.isArray(t)?t:[t]:[]}function Ic(t,n){return Array.isArray(t)?t.includes(n):t===n}function c_(t,n){let e=Xu(n);return Xu(t).forEach(a=>{Ic(e,a)||e.push(a)}),e}function l_(t,n){return Xu(n).filter(e=>!Ic(t,e))}var Nc=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=ap(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=rp(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,e){return this.control?this.control.hasError(n,e):!1}getError(n,e){return this.control?this.control.getError(n,e):null}},Zi=class extends Nc{name;get formDirective(){return null}get path(){return null}};var Yr="VALID",Ec="INVALID",Va="PENDING",Zr="DISABLED",di=class{},Mc=class extends di{value;source;constructor(n,e){super(),this.value=n,this.source=e}},Qr=class extends di{pristine;source;constructor(n,e){super(),this.pristine=n,this.source=e}},Xr=class extends di{touched;source;constructor(n,e){super(),this.touched=n,this.source=e}},Ba=class extends di{status;source;constructor(n,e){super(),this.status=n,this.source=e}},Tc=class extends di{source;constructor(n){super(),this.source=n}},za=class extends di{source;constructor(n){super(),this.source=n}};function D_(t){return(Pc(t)?t.validators:t)||null}function QD(t){return Array.isArray(t)?ap(t):t||null}function I_(t,n){return(Pc(n)?n.asyncValidators:t)||null}function XD(t){return Array.isArray(t)?rp(t):t||null}function Pc(t){return t!=null&&!Array.isArray(t)&&typeof t=="object"}function JD(t,n,e){let i=t.controls;if(!(n?Object.keys(i):i).length)throw new H(1e3,"");if(!N_(i,e))throw new H(1001,"")}function eI(t,n,e){t._forEachChild((i,a)=>{if(e[a]===void 0)throw new H(-1002,"")})}var Ac=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_hasRequired=ae(!1);_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,e){this._assignValidators(n),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n,this._updateHasRequiredValidator()}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return tt(this.statusReactive)}set status(n){tt(()=>this.statusReactive.set(n))}_status=at(()=>this.statusReactive());statusReactive=ae(void 0);get valid(){return this.status===Yr}get invalid(){return this.status===Ec}get pending(){return this.status===Va}get disabled(){return this.status===Zr}get enabled(){return this.status!==Zr}errors;get pristine(){return tt(this.pristineReactive)}set pristine(n){tt(()=>this.pristineReactive.set(n))}_pristine=at(()=>this.pristineReactive());pristineReactive=ae(!0);get dirty(){return!this.pristine}get touched(){return tt(this.touchedReactive)}set touched(n){tt(()=>this.touchedReactive.set(n))}_touched=at(()=>this.touchedReactive());touchedReactive=ae(!1);get untouched(){return!this.touched}_events=new E;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(c_(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(c_(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(l_(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(l_(n,this._rawAsyncValidators))}hasValidator(n){return Ic(this._rawValidators,n)}hasAsyncValidator(n){return Ic(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let e=this.touched===!1;this.touched=!0;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(ge(P({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new Xr(!0,i))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(n))}markAsUntouched(n={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=n.sourceControl??this;this._forEachChild(a=>{a.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:i})}),n.onlySelf||this._parent?._updateTouched(n,i),e&&n.emitEvent!==!1&&this._events.next(new Xr(!1,i))}markAsDirty(n={}){let e=this.pristine===!0;this.pristine=!1;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(ge(P({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new Qr(!1,i))}markAsPristine(n={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=n.sourceControl??this;this._forEachChild(a=>{a.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,i),e&&n.emitEvent!==!1&&this._events.next(new Qr(!0,i))}markAsPending(n={}){this.status=Va;let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Ba(this.status,e)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(ge(P({},n),{sourceControl:e}))}disable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=Zr,this.errors=null,this._forEachChild(a=>{a.disable(ge(P({},n),{onlySelf:!0}))}),this._updateValue();let i=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Mc(this.value,i)),this._events.next(new Ba(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(ge(P({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(a=>a(!0))}enable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=Yr,this._forEachChild(i=>{i.enable(ge(P({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(ge(P({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(n,e){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===Yr||this.status===Va)&&this._runAsyncValidator(i,n.emitEvent)}let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Mc(this.value,e)),this._events.next(new Ba(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(ge(P({},n),{sourceControl:e}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?Zr:Yr}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,e){if(this.asyncValidator){this.status=Va,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:n!==!1};let i=__(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(a=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(a,{emitEvent:e,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,e={}){this.errors=n,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(n){let e=n;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((i,a)=>i&&i._find(a),this)}getError(n,e){let i=e?this.get(e):this;return i?.errors?i.errors[n]:null}hasError(n,e){return!!this.getError(n,e)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,e,i){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||i)&&this._events.next(new Ba(this.status,e)),this._parent&&this._parent._updateControlsErrors(n,e,i)}_initObservables(){this.valueChanges=new z,this.statusChanges=new z}_calculateStatus(){return this._allControlsDisabled()?Zr:this.errors?Ec:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(Va)?Va:this._anyControlsHaveStatus(Ec)?Ec:Yr}_anyControlsHaveStatus(n){return this._anyControls(e=>e.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,e){let i=!this._anyControlsDirty(),a=this.pristine!==i;this.pristine=i,n.onlySelf||this._parent?._updatePristine(n,e),a&&this._events.next(new Qr(this.pristine,e))}_updateTouched(n={},e){this.touched=this._anyControlsTouched(),this._events.next(new Xr(this.touched,e)),n.onlySelf||this._parent?._updateTouched(n,e)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){Pc(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=QD(this._rawValidators),this._updateHasRequiredValidator()}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=XD(this._rawAsyncValidators)}_updateHasRequiredValidator(){tt(()=>this._hasRequired.set(this.hasValidator(ja.required)))}};function N_(t,n){return Object.hasOwn(t,n)}function tI(t){return t.tagName==="INPUT"||t.tagName==="SELECT"||t.tagName==="TEXTAREA"}function nI(t,n,e,i){switch(e){case"name":t.setAttribute(n,e,i);break;case"disabled":case"readonly":case"required":i?t.setAttribute(n,e,""):t.removeAttribute(n,e);break;case"max":case"min":case"minLength":case"maxLength":i!==void 0?t.setAttribute(n,e,i.toString()):t.removeAttribute(n,e);break}}var Ju=class{kind;context;control;message;constructor({kind:n,context:e,control:i}){this.kind=n,this.context=e,this.control=i}};function iI(t){return typeof t=="number"?t:parseInt(t,10)}var M_=(()=>{class t{_validator=Dc;_onChange;_enabled;ngOnChanges(e){if(this.inputName in e){let i=this.normalizeInput(e[this.inputName].currentValue);this._enabled=this.enabled(i),this._validator=this._enabled?this.createValidator(i):Dc,this._onChange?.()}}validate(e){return this._validator(e)}registerOnValidatorChange(e){this._onChange=e}enabled(e){return e!=null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,features:[rt]})}return t})();var aI={provide:Ki,useExisting:ze(()=>T_),multi:!0};var T_=(()=>{class t extends M_{required;inputName="required";normalizeInput=I;createValidator=e=>g_;enabled(e){return e}static \u0275fac=(()=>{let e;return function(a){return(e||(e=Fe(t)))(a||t)}})();static \u0275dir=D({type:t,selectors:[["","required","","formControlName","",3,"type","checkbox"],["","required","","formControl","",3,"type","checkbox"],["","required","","ngModel","",3,"type","checkbox"]],hostVars:1,hostBindings:function(i,a){i&2&&K("required",a._enabled?"":null)},inputs:{required:"required"},standalone:!1,features:[_e([aI]),le]})}return t})();var rI={provide:Ki,useExisting:ze(()=>op),multi:!0},op=(()=>{class t extends M_{maxlength;inputName="maxlength";normalizeInput=e=>iI(e);createValidator=e=>b_(e);static \u0275fac=(()=>{let e;return function(a){return(e||(e=Fe(t)))(a||t)}})();static \u0275dir=D({type:t,selectors:[["","maxlength","","formControlName",""],["","maxlength","","formControl",""],["","maxlength","","ngModel",""]],hostVars:1,hostBindings:function(i,a){i&2&&K("maxlength",a._enabled?a.maxlength:null)},inputs:{maxlength:"maxlength"},standalone:!1,features:[_e([rI]),le]})}return t})();var oI=new y(""),Vc=new y("",{factory:()=>sp}),sp="always";function sI(t,n){return[...n.path,t]}function d_(t,n,e=sp){cp(t,n),n.valueAccessor.writeValue(t.value),(t.disabled||e==="always")&&n.valueAccessor.setDisabledState?.(t.disabled),lI(t,n),mI(t,n),dI(t,n),cI(t,n)}function m_(t,n,e=!0){let i=()=>{};n?.valueAccessor?.registerOnChange(i),n?.valueAccessor?.registerOnTouched(i),Oc(t,n),t&&(n._invokeOnDestroyCallbacks(),t._registerOnCollectionChange(()=>{}))}function Rc(t,n){t.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(n)})}function cI(t,n){if(n.valueAccessor.setDisabledState){let e=i=>{n.valueAccessor.setDisabledState(i)};t.registerOnDisabledChange(e),n._registerOnDestroy(()=>{t._unregisterOnDisabledChange(e)})}}function cp(t,n){let e=S_(t);n.validator!==null?t.setValidators(s_(e,n.validator)):typeof e=="function"&&t.setValidators([e]);let i=E_(t);n.asyncValidator!==null?t.setAsyncValidators(s_(i,n.asyncValidator)):typeof i=="function"&&t.setAsyncValidators([i]);let a=()=>t.updateValueAndValidity();Rc(n._rawValidators,a),Rc(n._rawAsyncValidators,a)}function Oc(t,n){let e=!1;if(t!==null){if(n.validator!==null){let a=S_(t);if(Array.isArray(a)&&a.length>0){let r=a.filter(o=>o!==n.validator);r.length!==a.length&&(e=!0,t.setValidators(r))}}if(n.asyncValidator!==null){let a=E_(t);if(Array.isArray(a)&&a.length>0){let r=a.filter(o=>o!==n.asyncValidator);r.length!==a.length&&(e=!0,t.setAsyncValidators(r))}}}let i=()=>{};return Rc(n._rawValidators,i),Rc(n._rawAsyncValidators,i),e}function lI(t,n){n.valueAccessor.registerOnChange(e=>{t._pendingValue=e,t._pendingChange=!0,t._pendingDirty=!0,t.updateOn==="change"&&A_(t,n)})}function dI(t,n){n.valueAccessor.registerOnTouched(()=>{t._pendingTouched=!0,t.updateOn==="blur"&&t._pendingChange&&A_(t,n),t.updateOn!=="submit"&&t.markAsTouched()})}function A_(t,n){t._pendingDirty&&t.markAsDirty(),t.setValue(t._pendingValue,{emitModelToViewChange:!1}),n.viewToModelUpdate(t._pendingValue),t._pendingChange=!1}function mI(t,n){let e=(i,a)=>{n.valueAccessor.writeValue(i),a&&n.viewToModelUpdate(i)};t.registerOnChange(e),n._registerOnDestroy(()=>{t._unregisterOnChange(e)})}function R_(t,n){t==null,cp(t,n)}function uI(t,n){return Oc(t,n)}function pI(t,n){if(!Object.hasOwn(t,"model"))return!1;let e=t.model;return e.isFirstChange()?!0:!Object.is(n,e.currentValue)}function hI(t){return Object.getPrototypeOf(t.constructor)===BD}function O_(t,n){t._syncPendingControls(),n.forEach(e=>{let i=e.control;i.updateOn==="submit"&&i._pendingChange&&(e.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function fI(t,n){if(!n)return null;Array.isArray(n);let e,i,a;return n.forEach(r=>{r.constructor===Lc?e=r:hI(r)?i=r:a=r}),a||i||e||null}function gI(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}var bI={provide:oI,useFactory:()=>{let t=m($n,{self:!0});return{setParseErrors:n=>{t.setParseErrorSource(n)},set onReset(n){t.onReset=n}}}},$n=class extends Nc{_parent=null;name=null;valueAccessor=null;isCustomControlBased=!1;userOnReset;resetSubscription;set onReset(n){this.userOnReset=n,this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.control&&(this.resetSubscription=this.control.events.subscribe(e=>{e instanceof za&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription?.add(this.resetSubscription))}isNativeFormElement=!1;rawValueAccessors;_selectedValueAccessor=null;get selectedValueAccessor(){return this._selectedValueAccessor??=fI(this,this.rawValueAccessors)}parseErrorsValidator=null;renderer;injector;requiredValidatorViaDi;subscription;customControlBindings=null;pendingCustomValue=void 0;hasPendingCustomValue=!1;constructor(n,e,i){super(),this.injector=n,this.renderer=e,this.rawValueAccessors=i,this.injector?.get(Et)?.onDestroy(()=>{this.removeParseErrorsValidator(this.control),this.subscription?.unsubscribe()})}setupCustomControl(){this.subscription?.unsubscribe();let n=this.control,e=this.injector?.get(Ie);if(!n||!e)return;let i=e.markForCheck.bind(e);if(this.subscription=new pe,this.subscription.add(()=>this.discardPendingCustomValue(n)),typeof n.registerOnChange=="function"){let a=(r,o)=>{this.discardPendingCustomValue(n),o&&this.viewToModelUpdate(r),i()};n.registerOnChange(a),this.subscription.add(()=>n._unregisterOnChange(a))}this.subscription.add(n.valueChanges.subscribe(i)),this.subscription.add(n.statusChanges.subscribe(i)),this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.userOnReset&&(this.resetSubscription=n.events.subscribe(a=>{a instanceof za&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription.add(this.resetSubscription)),this.parseErrorsValidator&&n.addValidators(this.parseErrorsValidator)}ngControlCreate(n){!n.nativeElement.hasAttribute?.("ngNoCva")&&(this.rawValueAccessors&&this.rawValueAccessors.length>0||this.valueAccessor!==null)||!n.customControl||(this.isCustomControlBased=!0,n.listenToCustomControlModel(a=>{let r=this.control;r&&(r._pendingValue=a,r._pendingChange=!0,r._pendingDirty=!0,this.pendingCustomValue=a,this.hasPendingCustomValue=!0,r.updateOn==="change"&&this.updateControlFromCustom(r))}),n.listenToCustomControlOutput("touch",()=>{let a=this.control;a&&(a._pendingTouched=!0,a.updateOn==="blur"&&a._pendingChange&&this.updateControlFromCustom(a),a.updateOn!=="submit"&&a.markAsTouched())}),this.customControlBindings={},this.isNativeFormElement=tI(n.nativeElement),this.requiredValidatorViaDi=this._rawValidators.find(a=>a instanceof T_))}updateControlFromCustom(n){let e=n._pendingValue;n._pendingChange=!1,this.clearPendingCustomValue(),this.customControlBindings&&(this.customControlBindings.value=e),n._pendingDirty&&n.markAsDirty(),n.setValue(e,{emitModelToViewChange:!1}),this.viewToModelUpdate(n.value)}discardPendingCustomValue(n){this.hasPendingCustomValue&&(this.customControlBindings&&(this.customControlBindings.value=this.pendingCustomValue),this.clearPendingCustomValue(),n._pendingValue=n.value,n._pendingChange=!1,n._pendingDirty=!1)}clearPendingCustomValue(){this.pendingCustomValue=void 0,this.hasPendingCustomValue=!1}ngControlUpdate(n,e){if(!this.isCustomControlBased)return;let i=this.control,a=this.customControlBindings;if(!i||!a)return;this.hasPendingCustomValue&&!i._pendingChange&&(a.value=this.pendingCustomValue,this.clearPendingCustomValue()),Object.is(a.value,i.value)||(this.discardPendingCustomValue(i),a.value=i.value,n.setCustomControlModelInput(i.value)),this.bindControlProperty(n,a,"touched",i.touched),this.bindControlProperty(n,a,"dirty",i.dirty),this.bindControlProperty(n,a,"valid",i.valid),this.bindControlProperty(n,a,"invalid",i.invalid),this.bindControlProperty(n,a,"pending",i.pending),this.bindControlProperty(n,a,"disabled",i.disabled),this.shouldBindRequired&&this.bindControlProperty(n,a,"required",this.isRequired);let r=i.errors;if(a.errors!==r){a.errors=r;let o=this._convertErrors(r);n.setInputOnDirectives("errors",o)}}get isRequired(){return(this.requiredValidatorViaDi?._enabled||this.control?._hasRequired())??!1}get shouldBindRequired(){return!0}bindControlProperty(n,e,i,a){if(e[i]===a)return;e[i]=a;let r=n.setInputOnDirectives(i,a);this.isNativeFormElement&&!r&&(i==="disabled"||i==="required")&&this.renderer&&nI(this.renderer,n.nativeElement,i,a)}_convertErrors(n){if(n===null)return[];let e=this.control;return Object.entries(n).map(([i,a])=>new Ju({context:a,kind:i,control:e}))}setParseErrorSource(n){if(n===void 0)return;let e=null,i=at(()=>{let a=n();return a.length===0?null:a.reduce((r,o)=>(r[o.kind]=o,r),{})});this.parseErrorsValidator=(()=>e).bind(this),Jt(()=>{e=i(),this.control?.updateValueAndValidity({emitEvent:!1})},{injector:this.injector})}removeParseErrorsValidator(n){this.parseErrorsValidator&&(n?.removeValidators(this.parseErrorsValidator),n?.updateValueAndValidity({emitEvent:!1}))}},ep=class{_cd;constructor(n){this._cd=n}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}};var F_=(()=>{class t extends ep{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(Ae($n,2))};static \u0275dir=D({type:t,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,a){i&2&&S("ng-untouched",a.isUntouched)("ng-touched",a.isTouched)("ng-pristine",a.isPristine)("ng-dirty",a.isDirty)("ng-valid",a.isValid)("ng-invalid",a.isInvalid)("ng-pending",a.isPending)},standalone:!1,features:[le]})}return t})();var Fc=class extends Ac{constructor(n,e,i){super(D_(e),I_(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,e){let i=this._find(n);return i||(this.controls[n]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(n,e,i={}){this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(n,e={}){let i=this._find(n);i&&i._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(n,e,i={}){let a=this._find(n);a&&a._registerOnCollectionChange(()=>{}),delete this.controls[n],e&&this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(n){return this._find(n)?.enabled===!0}setValue(n,e={}){tt(()=>{eI(this,!0,n),Object.keys(n).forEach(i=>{JD(this,!0,i),this.controls[i].setValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)})}patchValue(n,e={}){n!=null&&(Object.keys(n).forEach(i=>{let a=this._find(i);a&&a.patchValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n={},e={}){this._forEachChild((i,a)=>{i.reset(n?n[a]:null,ge(P({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new za(this))}getRawValue(){return this._reduceChildren({},(n,e,i)=>(n[i]=e.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(e,i)=>i._syncPendingControls()?!0:e);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(e=>{let i=this.controls[e];i&&n(i,e)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[e,i]of Object.entries(this.controls))if(this.contains(e)&&n(i))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(e,i,a)=>((i.enabled||this.disabled)&&(e[a]=i.value),e))}_reduceChildren(n,e){let i=n;return this._forEachChild((a,r)=>{i=e(i,a,r)}),i}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return N_(this.controls,n)?this.controls[n]:null}};var vI={provide:Zi,useExisting:ze(()=>Bc)},Kr=Promise.resolve(),Bc=(()=>{class t extends Zi{callSetDisabledState;get submitted(){return tt(this.submittedReactive)}_submitted=at(()=>this.submittedReactive());submittedReactive=ae(!1);_directives=new Set;form;ngSubmit=new z;options;constructor(e,i,a){super(),this.callSetDisabledState=a,this.form=new Fc({},ap(e),rp(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){Kr.then(()=>{let i=this._findContainer(e.path);e.control=i.registerControl(e.name,e.control),e._setupWithForm(this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){Kr.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){Kr.then(()=>{let i=this._findContainer(e.path),a=new Fc({});R_(a,e),i.registerControl(e.name,a),a.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){Kr.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,i){Kr.then(()=>{this.form.get(e.path).setValue(i)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),O_(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new Tc(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(i){return new(i||t)(Ae(Ki,10),Ae(ip,10),Ae(Vc,8))};static \u0275dir=D({type:t,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,a){i&1&&C("submit",function(o){return a.onSubmit(o)})("reset",function(){return a.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[_e([vI]),le]})}return t})();function u_(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function p_(t){return typeof t=="object"&&t!==null&&Object.keys(t).length===2&&"value"in t&&"disabled"in t}var L_=class extends Ac{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,e,i){super(D_(e),I_(i,e)),this._applyFormState(n),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),Pc(e)&&(e.nonNullable||e.initialValueIsDefault)&&(p_(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,e={}){tt(()=>{this.value=this._pendingValue=n,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)})}patchValue(n,e={}){this.setValue(n,e)}reset(n=this.defaultValue,e={}){this._applyFormState(n),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new za(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){u_(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){u_(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){p_(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var _I=t=>t instanceof L_;var yI=(()=>{class t extends Zi{callSetDisabledState;get submitted(){return tt(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=at(()=>this._submittedReactive());_submittedReactive=ae(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,i,a){super(),this.callSetDisabledState=a,this._setValidators(e),this._setAsyncValidators(i)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){if(!this.form)throw new H(1052,!1);Object.hasOwn(e,"form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(Oc(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let i=this.form.get(e.path);if(!i)throw new H(-1055,!1);return e._setupWithForm(i,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),i}getControl(e){return this.form.get(e.path)}removeControl(e){m_(e.control||null,e,!1),gI(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,i){this.form.get(e.path).setValue(i)}onReset(){this.resetForm()}resetForm(e=void 0,i={}){this.form.reset(e,i),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,O_(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new Tc(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let i=e.control,a=this.form.get(e.path);i!==a&&(m_(i||null,e),_I(a)&&e._setupWithForm(a,this.callSetDisabledState))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let i=this.form.get(e.path);R_(i,e),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let i=this.form?.get(e.path);i&&uI(i,e)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){cp(this.form,this),this._oldForm&&Oc(this._oldForm,this)}static \u0275fac=function(i){return new(i||t)(Ae(Ki,10),Ae(ip,10),Ae(Vc,8))};static \u0275dir=D({type:t,features:[le,rt]})}return t})(),xI={provide:Zi,useExisting:ze(()=>jc)},jc=(()=>{class t extends yI{form=null;ngSubmit=new z;get control(){return this.form}static \u0275fac=(()=>{let e;return function(a){return(e||(e=Fe(t)))(a||t)}})();static \u0275dir=D({type:t,selectors:[["","formGroup",""]],hostBindings:function(i,a){i&1&&C("submit",function(o){return a.onSubmit(o)})("reset",function(){return a.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[_e([xI]),le]})}return t})(),wI={provide:$n,useExisting:ze(()=>lp)},h_=Promise.resolve(),lp=(()=>{class t extends $n{_changeDetectorRef;callSetDisabledState;control=new L_;static ngAcceptInputType_isDisabled;_registered=!1;_ngModelInjector;viewModel;name="";isDisabled;model;options;update=new z;constructor(e,i,a,r,o,s,c,l){super(c,l,r),this._changeDetectorRef=o,this.callSetDisabledState=s,this._parent=e,this._setValidators(i),this._setAsyncValidators(a)}ngOnChanges(e){if(this._registered,this._checkForErrors(),!this._registered||"name"in e){if(this._registered&&(this._checkName(),this.formDirective)){let i=e.name.previousValue;this.formDirective.removeControl({name:i,path:this._getPath(i)})}this._setUpControl()}"isDisabled"in e&&this._updateDisabled(e),pI(e,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective?.removeControl(this)}\u0275ngControlCreate(e){super.ngControlCreate(e)}\u0275ngControlUpdate(e){super.ngControlUpdate(e,!1)}get shouldBindRequired(){return!1}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){this.isCustomControlBased?this.setupCustomControl():(this.valueAccessor??=this.selectedValueAccessor,d_(this.control,this,this.callSetDisabledState)),this.control.updateValueAndValidity({emitEvent:!1})}_setupWithForm(e){this.isCustomControlBased?this.setupCustomControl():(this.valueAccessor??=this.selectedValueAccessor,d_(this.control,this,e))}_checkForErrors(){this._checkName()}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(e){h_.then(()=>{this.control.setValue(e,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(e){let i=e.isDisabled.currentValue,a=i!==0&&I(i);h_.then(()=>{a&&!this.control.disabled?this.control.disable():!a&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(e){return this._parent?sI(e,this._parent):[e]}static \u0275fac=function(i){return new(i||t)(Ae(Zi,9),Ae(Ki,10),Ae(ip,10),Ae(Cn,10),Ae(Ie,8),Ae(Vc,8),Ae(X,8),Ae(Se,8))};static \u0275dir=D({type:t,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],standalone:!1,features:[_e([wI,bI]),le,rt,Su(null)]})}return t})();var kI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=G({})}return t})();var P_=(()=>{class t{static withConfig(e){return{ngModule:t,providers:[{provide:Vc,useValue:e.callSetDisabledState??sp}]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=G({imports:[kI]})}return t})();function Jr(t){return t.buttons===0||t.detail===0}function eo(t){let n=t.touches&&t.touches[0]||t.changedTouches&&t.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var dp;function V_(){if(dp==null){let t=typeof document<"u"?document.head:null;dp=!!(t&&(t.createShadowRoot||t.attachShadow))}return dp}function mp(t){if(V_()){let n=t.getRootNode?t.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function Nt(t){if(t.composedPath)try{return t.composedPath()[0]}catch{}return t.target}var up;try{up=typeof Intl<"u"&&Intl.v8BreakIterator}catch{up=!1}var ye=(()=>{class t{_platformId=m(ji);isBrowser=this._platformId?Xv(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||up)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;static \u0275fac=function(i){return new(i||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})}return t})();var to;function B_(){if(to==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>to=!0}))}finally{to=to||!1}return to}function Ha(t){return B_()?t:!!t.capture}function Qi(t,n=0){return j_(t)?Number(t):arguments.length===2?n:0}function j_(t){return!isNaN(parseFloat(t))&&!isNaN(Number(t))}function wt(t){return t instanceof L?t.nativeElement:t}var z_=new y("cdk-input-modality-detector-options"),H_={ignoreKeys:[18,17,224,91,16]},U_=650,pp={passive:!0,capture:!0},$_=(()=>{class t{_platform=m(ye);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new Kn(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(i=>i===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=Nt(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<U_||(this._modality.next(Jr(e)?"keyboard":"mouse"),this._mostRecentTarget=Nt(e))};_onTouchstart=e=>{if(eo(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=Nt(e)};constructor(){let e=m(N),i=m(ee),a=m(z_,{optional:!0});if(this._options=P(P({},H_),a),this.modalityDetected=this._modality.pipe(xi(1)),this.modalityChanged=this.modalityDetected.pipe(Ko()),this._platform.isBrowser){let r=m(Je).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[r.listen(i,"keydown",this._onKeydown,pp),r.listen(i,"mousedown",this._onMousedown,pp),r.listen(i,"touchstart",this._onTouchstart,pp)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(i){return new(i||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})}return t})(),no=(function(t){return t[t.IMMEDIATE=0]="IMMEDIATE",t[t.EVENTUAL=1]="EVENTUAL",t})(no||{}),G_=new y("cdk-focus-monitor-default-options"),zc=Ha({passive:!0,capture:!0}),on=(()=>{class t{_ngZone=m(N);_platform=m(ye);_inputModalityDetector=m($_);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=m(ee);_stopInputModalityDetector=new E;constructor(){let e=m(G_,{optional:!0});this._detectionMode=e?.detectionMode||no.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let i=Nt(e);for(let a=i;a;a=a.parentElement)e.type==="focus"?this._onFocus(e,a):this._onBlur(e,a)};monitor(e,i=!1){let a=wt(e);if(!this._platform.isBrowser||a.nodeType!==1)return In();let r=mp(a)||this._document,o=this._elementInfo.get(a);if(o)return i&&(o.checkChildren=!0),o.subject;let s={checkChildren:i,subject:new E,rootNode:r};return this._elementInfo.set(a,s),this._registerGlobalListeners(s),s.subject}stopMonitoring(e){let i=wt(e),a=this._elementInfo.get(i);a&&(a.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(a))}focusVia(e,i,a){let r=wt(e),o=this._document.activeElement;r===o?this._getClosestElementsInfo(r).forEach(([s,c])=>this._originChanged(s,i,c)):(this._setOrigin(i),typeof r.focus=="function"&&r.focus(a))}ngOnDestroy(){this._elementInfo.forEach((e,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===no.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,i){e.classList.toggle("cdk-focused",!!i),e.classList.toggle("cdk-touch-focused",i==="touch"),e.classList.toggle("cdk-keyboard-focused",i==="keyboard"),e.classList.toggle("cdk-mouse-focused",i==="mouse"),e.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(e,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&i,this._detectionMode===no.IMMEDIATE){clearTimeout(this._originTimeoutId);let a=this._originFromTouchInteraction?U_:1;this._originTimeoutId=setTimeout(()=>this._origin=null,a)}})}_onFocus(e,i){let a=this._elementInfo.get(i),r=Nt(e);!a||!a.checkChildren&&i!==r||this._originChanged(i,this._getFocusOrigin(r),a)}_onBlur(e,i){let a=this._elementInfo.get(i);!a||a.checkChildren&&e.relatedTarget instanceof Node&&i.contains(e.relatedTarget)||(this._setClasses(i),this._emitOrigin(a,null))}_emitOrigin(e,i){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(i))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let i=e.rootNode,a=this._rootNodeFocusListenerCount.get(i)||0;a||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,zc),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,zc)}),this._rootNodeFocusListenerCount.set(i,a+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(Oe(this._stopInputModalityDetector)).subscribe(r=>{this._setOrigin(r,!0)}))}_removeGlobalListeners(e){let i=e.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let a=this._rootNodeFocusListenerCount.get(i);a>1?this._rootNodeFocusListenerCount.set(i,a-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,zc),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,zc),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,i,a){this._setClasses(e,i),this._emitOrigin(a,i),this._lastFocusOrigin=i}_getClosestElementsInfo(e){let i=[];return this._elementInfo.forEach((a,r)=>{(r===e||a.checkChildren&&r.contains(e))&&i.push([r,a])}),i}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:i,mostRecentModality:a}=this._inputModalityDetector;if(a!=="mouse"||!i||i===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let r=e.labels;if(r){for(let o=0;o<r.length;o++)if(r[o].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})}return t})(),hp=(()=>{class t{_elementRef=m(L);_focusMonitor=m(on);_monitorSubscription;_focusOrigin=null;cdkFocusChange=new z;get focusOrigin(){return this._focusOrigin}ngAfterViewInit(){let e=this._elementRef.nativeElement;this._monitorSubscription=this._focusMonitor.monitor(e,e.nodeType===1&&e.hasAttribute("cdkMonitorSubtreeFocus")).subscribe(i=>{this._focusOrigin=i,this.cdkFocusChange.emit(i)})}ngOnDestroy(){this._focusMonitor.stopMonitoring(this._elementRef),this._monitorSubscription?.unsubscribe()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["","cdkMonitorElementFocus",""],["","cdkMonitorSubtreeFocus",""]],outputs:{cdkFocusChange:"cdkFocusChange"},exportAs:["cdkMonitorFocus"]})}return t})();var Hc=new WeakMap,je=(()=>{class t{_appRef;_injector=m(X);_environmentInjector=m(ct);load(e){let i=this._appRef=this._appRef||this._injector.get(rn),a=Hc.get(i);a||(a={loaders:new Set,refs:[]},Hc.set(i,a),i.onDestroy(()=>{Hc.get(i)?.refs.forEach(r=>r.destroy()),Hc.delete(i)})),a.loaders.has(e)||(a.loaders.add(e),a.refs.push(bc(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})}return t})();var fp=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=B({type:t,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,a){},styles:[`.cdk-visually-hidden {
  border: 0;
  clip: rect(0 0 0 0);
  height: 1px;
  margin: -1px;
  overflow: hidden;
  padding: 0;
  position: absolute;
  width: 1px;
  white-space: nowrap;
  outline: 0;
  -webkit-appearance: none;
  -moz-appearance: none;
  left: 0;
}
[dir=rtl] .cdk-visually-hidden {
  left: auto;
  right: 0;
}
`],encapsulation:2})}return t})(),Uc;function SI(){if(Uc===void 0&&(Uc=null,typeof window<"u")){let t=window;if(t.trustedTypes!==void 0)try{Uc=t.trustedTypes.createPolicy("angular#components",{createHTML:n=>n})}catch(n){console.error(n)}}return Uc}function EI(t){return SI()?.createHTML(t)||t}function W_(t,n,e){let i=e.sanitize(Rt.HTML,n);t.innerHTML=EI(i||"")}function Ua(t){return Array.isArray(t)?t:[t]}var q_=new Set,Xi,$c=(()=>{class t{_platform=m(ye);_nonce=m(zi,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):II}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&DI(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})}return t})();function DI(t,n){if(!q_.has(t))try{Xi||(Xi=document.createElement("style"),n&&Xi.setAttribute("nonce",n),Xi.setAttribute("type","text/css"),document.head.appendChild(Xi)),Xi.sheet&&(Xi.sheet.insertRule(`@media ${t.replace(/[{}]/g,"")} {body{ }}`,0),q_.add(t))}catch(e){console.error(e)}}function II(t){return{matches:t==="all"||t==="",media:t,addListener:()=>{},removeListener:()=>{}}}var gp=(()=>{class t{_mediaMatcher=m($c);_zone=m(N);_queries=new Map;_destroySubject=new E;ngOnDestroy(){this._destroySubject.next(),this._destroySubject.complete()}isMatched(e){return Y_(Ua(e)).some(a=>this._registerQuery(a).mql.matches)}observe(e){let a=Y_(Ua(e)).map(o=>this._registerQuery(o).observable),r=$l(a);return r=ua(r.pipe(ql(1)),r.pipe(xi(1),Nn(0))),r.pipe(Qe(o=>{let s={matches:!1,breakpoints:{}};return o.forEach(({matches:c,query:l})=>{s.matches=s.matches||c,s.breakpoints[l]=c}),s}))}_registerQuery(e){if(this._queries.has(e))return this._queries.get(e);let i=this._mediaMatcher.matchMedia(e),r={observable:new se(o=>{let s=c=>this._zone.run(()=>o.next(c));return i.addListener(s),()=>{i.removeListener(s)}}).pipe(yt(i),Qe(({matches:o})=>({query:e,matches:o})),Oe(this._destroySubject)),mql:i};return this._queries.set(e,r),r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})}return t})();function Y_(t){return t.map(n=>n.split(",")).reduce((n,e)=>n.concat(e)).map(n=>n.trim())}function NI(t){if(t.type==="characterData"&&t.target instanceof Comment)return!0;if(t.type==="childList"){for(let n=0;n<t.addedNodes.length;n++)if(!(t.addedNodes[n]instanceof Comment))return!1;for(let n=0;n<t.removedNodes.length;n++)if(!(t.removedNodes[n]instanceof Comment))return!1;return!0}return!1}var Z_=(()=>{class t{create(e){return typeof MutationObserver>"u"?null:new MutationObserver(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})}return t})(),MI=(()=>{class t{_mutationObserverFactory=m(Z_);_observedElements=new Map;_ngZone=m(N);ngOnDestroy(){this._observedElements.forEach((e,i)=>this._cleanupObserver(i))}observe(e){let i=wt(e);return new se(a=>{let o=this._observeElement(i).pipe(Qe(s=>s.filter(c=>!NI(c))),ot(s=>!!s.length)).subscribe(s=>{this._ngZone.run(()=>{a.next(s)})});return()=>{o.unsubscribe(),this._unobserveElement(i)}})}_observeElement(e){return this._ngZone.runOutsideAngular(()=>{if(this._observedElements.has(e))this._observedElements.get(e).count++;else{let i=new E,a=this._mutationObserverFactory.create(r=>i.next(r));a&&a.observe(e,{characterData:!0,childList:!0,subtree:!0}),this._observedElements.set(e,{observer:a,stream:i,count:1})}return this._observedElements.get(e).stream})}_unobserveElement(e){this._observedElements.has(e)&&(this._observedElements.get(e).count--,this._observedElements.get(e).count||this._cleanupObserver(e))}_cleanupObserver(e){if(this._observedElements.has(e)){let{observer:i,stream:a}=this._observedElements.get(e);i&&i.disconnect(),a.complete(),this._observedElements.delete(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})}return t})(),Gc=(()=>{class t{_contentObserver=m(MI);_elementRef=m(L);event=new z;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._disabled?this._unsubscribe():this._subscribe()}_disabled=!1;get debounce(){return this._debounce}set debounce(e){this._debounce=Qi(e),this._subscribe()}_debounce;_currentSubscription=null;ngAfterContentInit(){!this._currentSubscription&&!this.disabled&&this._subscribe()}ngOnDestroy(){this._unsubscribe()}_subscribe(){this._unsubscribe();let e=this._contentObserver.observe(this._elementRef);this._currentSubscription=(this.debounce?e.pipe(Nn(this.debounce)):e).subscribe(this.event)}_unsubscribe(){this._currentSubscription?.unsubscribe()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["","cdkObserveContent",""]],inputs:{disabled:[2,"cdkObserveContentDisabled","disabled",I],debounce:"debounce"},outputs:{event:"cdkObserveContent"},exportAs:["cdkObserveContent"]})}return t})(),Wc=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=G({providers:[Z_]})}return t})();var K_=new y("liveAnnouncerElement",{providedIn:"root",factory:()=>null}),Q_=new y("LIVE_ANNOUNCER_DEFAULT_OPTIONS"),TI=0,bp=(()=>{class t{_ngZone=m(N);_defaultOptions=m(Q_,{optional:!0});_liveElement;_document=m(ee);_sanitizer=m(Qu);_previousTimeout;_currentPromise;_currentResolve;constructor(){let e=m(K_,{optional:!0});this._liveElement=e||this._createLiveElement()}announce(e,...i){let a=this._defaultOptions,r,o;return i.length===1&&typeof i[0]=="number"?o=i[0]:[r,o]=i,this.clear(),clearTimeout(this._previousTimeout),r||(r=a&&a.politeness?a.politeness:"polite"),o==null&&a&&(o=a.duration),this._liveElement.setAttribute("aria-live",r),this._liveElement.id&&this._exposeAnnouncerToModals(this._liveElement.id),this._ngZone.runOutsideAngular(()=>(this._currentPromise||(this._currentPromise=new Promise(s=>this._currentResolve=s)),clearTimeout(this._previousTimeout),this._previousTimeout=setTimeout(()=>{!e||typeof e=="string"?this._liveElement.textContent=e:W_(this._liveElement,e,this._sanitizer),typeof o=="number"&&(this._previousTimeout=setTimeout(()=>this.clear(),o)),this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0},100),this._currentPromise))}clear(){this._liveElement&&(this._liveElement.textContent="")}ngOnDestroy(){clearTimeout(this._previousTimeout),this._liveElement?.remove(),this._liveElement=null,this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0}_createLiveElement(){let e="cdk-live-announcer-element",i=this._document.getElementsByClassName(e),a=this._document.createElement("div");for(let r=0;r<i.length;r++)i[r].remove();return a.classList.add(e),a.classList.add("cdk-visually-hidden"),a.setAttribute("aria-atomic","true"),a.setAttribute("aria-live","polite"),a.id=`cdk-live-announcer-${TI++}`,this._document.body.appendChild(a),a}_exposeAnnouncerToModals(e){let i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let a=0;a<i.length;a++){let r=i[a],o=r.getAttribute("aria-owns");o?o.indexOf(e)===-1&&r.setAttribute("aria-owns",o+" "+e):r.setAttribute("aria-owns",e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})}return t})();var AI=200,qc=class{_letterKeyStream=new E;_items=[];_selectedItemIndex=-1;_pressedLetters=[];_skipPredicateFn;_selectedItem=new E;selectedItem=this._selectedItem;constructor(n,e){let i=typeof e?.debounceInterval=="number"?e.debounceInterval:AI;e?.skipPredicate&&(this._skipPredicateFn=e.skipPredicate),this.setItems(n),this._setupKeyHandler(i)}destroy(){this._pressedLetters=[],this._letterKeyStream.complete(),this._selectedItem.complete()}setCurrentSelectedItemIndex(n){this._selectedItemIndex=n}setItems(n){this._items=n}handleKey(n){let e=n.keyCode;n.key&&n.key.length===1?this._letterKeyStream.next(n.key.toLocaleUpperCase()):(e>=65&&e<=90||e>=48&&e<=57)&&this._letterKeyStream.next(String.fromCharCode(e))}isTyping(){return this._pressedLetters.length>0}reset(){this._pressedLetters=[]}_setupKeyHandler(n){this._letterKeyStream.pipe(Zl(e=>this._pressedLetters.push(e)),Nn(n),ot(()=>this._pressedLetters.length>0),Qe(()=>this._pressedLetters.join("").toLocaleUpperCase())).subscribe(e=>{for(let i=1;i<this._items.length+1;i++){let a=(this._selectedItemIndex+i)%this._items.length,r=this._items[a];if(!this._skipPredicateFn?.(r)&&r.getLabel?.().toLocaleUpperCase().trim().indexOf(e)===0){this._selectedItem.next(r);break}}this._pressedLetters=[]})}};function $a(t,...n){return n.length?n.some(e=>t[e]):t.altKey||t.shiftKey||t.ctrlKey||t.metaKey}var Yc=class{_items;_activeItemIndex=ae(-1);_activeItem=ae(null);_wrap=!1;_typeaheadSubscription=pe.EMPTY;_itemChangesSubscription;_vertical=!0;_horizontal=null;_allowedModifierKeys=[];_homeAndEnd=!1;_pageUpAndDown={enabled:!1,delta:10};_effectRef;_typeahead;_skipPredicateFn=n=>n.disabled;constructor(n,e){this._items=n,n instanceof nn?this._itemChangesSubscription=n.changes.subscribe(i=>this._itemsChanged(i.toArray())):Lt(n)&&(this._effectRef=Jt(()=>this._itemsChanged(n()),{injector:e}))}tabOut=new E;change=new E;skipPredicate(n){return this._skipPredicateFn=n,this}withWrap(n=!0){return this._wrap=n,this}withVerticalOrientation(n=!0){return this._vertical=n,this}withHorizontalOrientation(n){return this._horizontal=n,this}withAllowedModifierKeys(n){return this._allowedModifierKeys=n,this}withTypeAhead(n=200){this._typeaheadSubscription.unsubscribe();let e=this._getItemsArray();return this._typeahead=new qc(e,{debounceInterval:typeof n=="number"?n:void 0,skipPredicate:i=>this._skipPredicateFn(i)}),this._typeaheadSubscription=this._typeahead.selectedItem.subscribe(i=>{this.setActiveItem(i)}),this}cancelTypeahead(){return this._typeahead?.reset(),this}withHomeAndEnd(n=!0){return this._homeAndEnd=n,this}withPageUpDown(n=!0,e=10){return this._pageUpAndDown={enabled:n,delta:e},this}setActiveItem(n){let e=this._activeItem();this.updateActiveItem(n),this._activeItem()!==e&&this.change.next(this._activeItemIndex())}onKeydown(n){let e=n.keyCode,a=["altKey","ctrlKey","metaKey","shiftKey"].every(r=>!n[r]||this._allowedModifierKeys.indexOf(r)>-1);switch(e){case 9:this.tabOut.next();return;case 40:if(this._vertical&&a){this.setNextItemActive();break}else return;case 38:if(this._vertical&&a){this.setPreviousItemActive();break}else return;case 39:if(this._horizontal&&a){this._horizontal==="rtl"?this.setPreviousItemActive():this.setNextItemActive();break}else return;case 37:if(this._horizontal&&a){this._horizontal==="rtl"?this.setNextItemActive():this.setPreviousItemActive();break}else return;case 36:if(this._homeAndEnd&&a){this.setFirstItemActive();break}else return;case 35:if(this._homeAndEnd&&a){this.setLastItemActive();break}else return;case 33:if(this._pageUpAndDown.enabled&&a){let r=this._activeItemIndex()-this._pageUpAndDown.delta;this._setActiveItemByIndex(r>0?r:0,1);break}else return;case 34:if(this._pageUpAndDown.enabled&&a){let r=this._activeItemIndex()+this._pageUpAndDown.delta,o=this._getItemsArray().length;this._setActiveItemByIndex(r<o?r:o-1,-1);break}else return;default:(a||$a(n,"shiftKey"))&&this._typeahead?.handleKey(n);return}this._typeahead?.reset(),n.preventDefault()}get activeItemIndex(){return this._activeItemIndex()}get activeItem(){return this._activeItem()}isTyping(){return!!this._typeahead&&this._typeahead.isTyping()}setFirstItemActive(){this._setActiveItemByIndex(0,1)}setLastItemActive(){this._setActiveItemByIndex(this._getItemsArray().length-1,-1)}setNextItemActive(){this._activeItemIndex()<0?this.setFirstItemActive():this._setActiveItemByDelta(1)}setPreviousItemActive(){this._activeItemIndex()<0&&this._wrap?this.setLastItemActive():this._setActiveItemByDelta(-1)}updateActiveItem(n){let e=this._getItemsArray(),i=typeof n=="number"?n:e.indexOf(n),a=e[i];this._activeItem.set(a??null),this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i)}destroy(){this._typeaheadSubscription.unsubscribe(),this._itemChangesSubscription?.unsubscribe(),this._effectRef?.destroy(),this._typeahead?.destroy(),this.tabOut.complete(),this.change.complete()}_setActiveItemByDelta(n){this._wrap?this._setActiveInWrapMode(n):this._setActiveInDefaultMode(n)}_setActiveInWrapMode(n){let e=this._getItemsArray();for(let i=1;i<=e.length;i++){let a=(this._activeItemIndex()+n*i+e.length)%e.length,r=e[a];if(!this._skipPredicateFn(r)){this.setActiveItem(a);return}}}_setActiveInDefaultMode(n){this._setActiveItemByIndex(this._activeItemIndex()+n,n)}_setActiveItemByIndex(n,e){let i=this._getItemsArray();if(i[n]){for(;this._skipPredicateFn(i[n]);)if(n+=e,!i[n])return;this.setActiveItem(n)}}_getItemsArray(){return Lt(this._items)?this._items():this._items instanceof nn?this._items.toArray():this._items}_itemsChanged(n){this._typeahead?.setItems(n);let e=this._activeItem();if(e){let i=n.indexOf(e);i>-1&&i!==this._activeItemIndex()&&(this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i))}}};var Ji=class extends Yc{_origin="program";setFocusOrigin(n){return this._origin=n,this}setActiveItem(n){super.setActiveItem(n),this.activeItem&&this.activeItem.focus(this._origin)}};var ny=new Map,Pe=class t{_appId=m(Bi);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,e=!1){this._appId!=="ng"&&(n+=this._appId);let i=ny.get(n);return i===void 0?i=0:i++,ny.set(n,i),`${n}${e?t._infix+"-":""}${i}`}static \u0275fac=function(e){return new(e||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})};var sn=(function(t){return t[t.NORMAL=0]="NORMAL",t[t.NEGATED=1]="NEGATED",t[t.INVERTED=2]="INVERTED",t})(sn||{}),Zc,ea;function Kc(){if(ea==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return ea=!1,ea;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)ea=!0;else{let t=Element.prototype.scrollTo;t?ea=!/\{\s*\[native code\]\s*\}/.test(t.toString()):ea=!1}}return ea}function Ga(){if(typeof document!="object"||!document)return sn.NORMAL;if(Zc==null){let t=document.createElement("div"),n=t.style;t.dir="rtl",n.width="1px",n.overflow="auto",n.visibility="hidden",n.pointerEvents="none",n.position="absolute";let e=document.createElement("div"),i=e.style;i.width="2px",i.height="1px",t.appendChild(e),document.body.appendChild(t),Zc=sn.NORMAL,t.scrollLeft===0&&(t.scrollLeft=1,Zc=t.scrollLeft===0?sn.NEGATED:sn.INVERTED),t.remove()}return Zc}function vp(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}var Wa,iy=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function _p(){if(Wa)return Wa;if(typeof document!="object"||!document)return Wa=new Set(iy),Wa;let t=document.createElement("input");return Wa=new Set(iy.filter(n=>(t.setAttribute("type",n),t.type===n))),Wa}var ay={XSmall:"(max-width: 599.98px)",Small:"(min-width: 600px) and (max-width: 959.98px)",Medium:"(min-width: 960px) and (max-width: 1279.98px)",Large:"(min-width: 1280px) and (max-width: 1919.98px)",XLarge:"(min-width: 1920px)",Handset:"(max-width: 599.98px) and (orientation: portrait), (max-width: 959.98px) and (orientation: landscape)",Tablet:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait), (min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",Web:"(min-width: 840px) and (orientation: portrait), (min-width: 1280px) and (orientation: landscape)",HandsetPortrait:"(max-width: 599.98px) and (orientation: portrait)",TabletPortrait:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait)",WebPortrait:"(min-width: 840px) and (orientation: portrait)",HandsetLandscape:"(max-width: 959.98px) and (orientation: landscape)",TabletLandscape:"(min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",WebLandscape:"(min-width: 1280px) and (orientation: landscape)"};var FI=new y("MATERIAL_ANIMATIONS"),ry=null;function yp(){return m(FI,{optional:!0})?.animationsDisabled||m(xr,{optional:!0})==="NoopAnimations"?"di-disabled":(ry??=m($c).matchMedia("(prefers-reduced-motion)").matches,ry?"reduced-motion":"enabled")}function Re(){return yp()!=="enabled"}function Ze(t){return t==null?"":typeof t=="string"?t:`${t}px`}function kt(t){return t!=null&&`${t}`!="false"}var qt=(function(t){return t[t.FADING_IN=0]="FADING_IN",t[t.VISIBLE=1]="VISIBLE",t[t.FADING_OUT=2]="FADING_OUT",t[t.HIDDEN=3]="HIDDEN",t})(qt||{}),xp=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=qt.HIDDEN;constructor(n,e,i,a=!1){this._renderer=n,this.element=e,this.config=i,this._animationForciblyDisabledThroughCss=a}fadeOut(){this._renderer.fadeOutRipple(this)}},oy=Ha({passive:!0,capture:!0}),wp=class{_events=new Map;addHandler(n,e,i,a){let r=this._events.get(e);if(r){let o=r.get(i);o?o.add(a):r.set(i,new Set([a]))}else this._events.set(e,new Map([[i,new Set([a])]])),n.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,oy)})}removeHandler(n,e,i){let a=this._events.get(n);if(!a)return;let r=a.get(e);r&&(r.delete(i),r.size===0&&a.delete(e),a.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,oy)))}_delegateEventHandler=n=>{let e=Nt(n);e&&this._events.get(n.type)?.forEach((i,a)=>{(a===e||a.contains(e))&&i.forEach(r=>r.handleEvent(n))})}},io={enterDuration:225,exitDuration:150},LI=800,sy=Ha({passive:!0,capture:!0}),cy=["mousedown","touchstart"],ly=["mouseup","mouseleave","touchend","touchcancel"],PI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=B({type:t,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,a){},styles:[`.mat-ripple {
  overflow: hidden;
  position: relative;
}
.mat-ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-ripple.mat-ripple-unbounded {
  overflow: visible;
}

.mat-ripple-element {
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
  transition: opacity, transform 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: scale3d(0, 0, 0);
  background-color: var(--%NS%mat-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 10%, transparent));
}
@media (forced-colors: active) {
  .mat-ripple-element {
    display: none;
  }
}
.cdk-drag-preview .mat-ripple-element, .cdk-drag-placeholder .mat-ripple-element {
  display: none;
}
`],encapsulation:2})}return t})(),ta=class t{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new wp;constructor(n,e,i,a,r){this._target=n,this._ngZone=e,this._platform=a,a.isBrowser&&(this._containerElement=wt(i)),r&&r.get(je).load(PI)}fadeInRipple(n,e,i={}){let a=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),r=P(P({},io),i.animation);i.centered&&(n=a.left+a.width/2,e=a.top+a.height/2);let o=i.radius||VI(n,e,a),s=n-a.left,c=e-a.top,l=r.enterDuration,d=document.createElement("div");d.classList.add("mat-ripple-element"),d.style.left=`${s-o}px`,d.style.top=`${c-o}px`,d.style.height=`${o*2}px`,d.style.width=`${o*2}px`,i.color!=null&&(d.style.backgroundColor=i.color),d.style.transitionDuration=`${l}ms`,this._containerElement.appendChild(d);let u=window.getComputedStyle(d),g=u.transitionProperty,f=u.transitionDuration,_=g==="none"||f==="0s"||f==="0s, 0s"||a.width===0&&a.height===0,w=new xp(this,d,i,_);d.style.transform="scale3d(1, 1, 1)",w.state=qt.FADING_IN,i.persistent||(this._mostRecentTransientRipple=w);let A=null;return!_&&(l||r.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let ie=()=>{A&&(A.fallbackTimer=null),clearTimeout(cn),this._finishRippleTransition(w)},fe=()=>this._destroyRipple(w),cn=setTimeout(fe,l+100);d.addEventListener("transitionend",ie),d.addEventListener("transitioncancel",fe),A={onTransitionEnd:ie,onTransitionCancel:fe,fallbackTimer:cn}}),this._activeRipples.set(w,A),(_||!l)&&this._finishRippleTransition(w),w}fadeOutRipple(n){if(n.state===qt.FADING_OUT||n.state===qt.HIDDEN)return;let e=n.element,i=P(P({},io),n.config.animation);e.style.transitionDuration=`${i.exitDuration}ms`,e.style.opacity="0",n.state=qt.FADING_OUT,(n._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let e=wt(n);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,cy.forEach(i=>{t._eventManager.addHandler(this._ngZone,i,e,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{ly.forEach(e=>{this._triggerElement.addEventListener(e,this,sy)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===qt.FADING_IN?this._startFadeOutTransition(n):n.state===qt.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let e=n===this._mostRecentTransientRipple,{persistent:i}=n.config;n.state=qt.VISIBLE,!i&&(!e||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let e=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=qt.HIDDEN,e!==null&&(n.element.removeEventListener("transitionend",e.onTransitionEnd),n.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),n.element.remove()}_onMousedown(n){let e=Jr(n),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+LI;!this._target.rippleDisabled&&!e&&!i&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!eo(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=n.changedTouches;if(e)for(let i=0;i<e.length;i++)this.fadeInRipple(e[i].clientX,e[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let e=n.state===qt.VISIBLE||n.config.terminateOnPointerUp&&n.state===qt.FADING_IN;!n.config.persistent&&e&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(cy.forEach(e=>t._eventManager.removeHandler(e,n,this)),this._pointerUpEventsRegistered&&(ly.forEach(e=>n.removeEventListener(e,this,sy)),this._pointerUpEventsRegistered=!1))}};function VI(t,n,e){let i=Math.max(Math.abs(t-e.left),Math.abs(t-e.right)),a=Math.max(Math.abs(n-e.top),Math.abs(n-e.bottom));return Math.sqrt(i*i+a*a)}var mi=new y("mat-ripple-global-options"),Gn=(()=>{class t{_elementRef=m(L);_animationsDisabled=Re();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(e){e&&this.fadeOutAllNonPersistent(),this._disabled=e,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(e){this._trigger=e,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let e=m(N),i=m(ye),a=m(mi,{optional:!0}),r=m(X);this._globalOptions=a||{},this._rippleRenderer=new ta(this,e,this._elementRef,i,r)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:P(P(P({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(e,i=0,a){return typeof e=="number"?this._rippleRenderer.fadeInRipple(e,i,P(P({},this.rippleConfig),a)):this._rippleRenderer.fadeInRipple(0,0,P(P({},this.rippleConfig),e))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,a){i&2&&S("mat-ripple-unbounded",a.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return t})();var BI={capture:!0},jI=["focus","mousedown","mouseenter","touchstart"],kp="mat-ripple-loader-uninitialized",Cp="mat-ripple-loader-class-name",dy="mat-ripple-loader-centered",Qc="mat-ripple-loader-disabled",Xc=(()=>{class t{_document=m(ee);_animationsDisabled=Re();_globalRippleOptions=m(mi,{optional:!0});_platform=m(ye);_ngZone=m(N);_injector=m(X);_eventCleanups;_hosts=new Map;constructor(){let e=m(Je).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>jI.map(i=>e.listen(this._document,i,this._onInteraction,BI)))}ngOnDestroy(){let e=this._hosts.keys();for(let i of e)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(e,i){e.setAttribute(kp,this._globalRippleOptions?.namespace??""),(i.className||!e.hasAttribute(Cp))&&e.setAttribute(Cp,i.className||""),i.centered&&e.setAttribute(dy,""),i.disabled&&e.setAttribute(Qc,"")}setDisabled(e,i){let a=this._hosts.get(e);a?(a.target.rippleDisabled=i,!i&&!a.hasSetUpEvents&&(a.hasSetUpEvents=!0,a.renderer.setupTriggerEvents(e))):i?e.setAttribute(Qc,""):e.removeAttribute(Qc)}_onInteraction=e=>{let i=Nt(e);if(i instanceof HTMLElement){let a=i.closest(`[${kp}="${this._globalRippleOptions?.namespace??""}"]`);a&&this._createRipple(a)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",e.getAttribute(Cp)),e.append(i);let a=this._globalRippleOptions,r=this._animationsDisabled?0:a?.animation?.enterDuration??io.enterDuration,o=this._animationsDisabled?0:a?.animation?.exitDuration??io.exitDuration,s={rippleDisabled:this._animationsDisabled||a?.disabled||e.hasAttribute(Qc),rippleConfig:{centered:e.hasAttribute(dy),terminateOnPointerUp:a?.terminateOnPointerUp,animation:{enterDuration:r,exitDuration:o}}},c=new ta(s,this._ngZone,i,this._platform,this._injector),l=!s.rippleDisabled;l&&c.setupTriggerEvents(e),this._hosts.set(e,{target:s,renderer:c,hasSetUpEvents:l}),e.removeAttribute(kp)}destroyRipple(e){let i=this._hosts.get(e);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})}return t})();var Ct=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=B({type:t,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,a){},styles:[`.mat-focus-indicator {
  position: relative;
}
.mat-focus-indicator::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  box-sizing: border-box;
  pointer-events: none;
  display: var(--%NS%mat-focus-indicator-display, none);
  border-width: var(--%NS%mat-focus-indicator-border-width, 3px);
  border-style: var(--%NS%mat-focus-indicator-border-style, solid);
  border-color: var(--%NS%mat-focus-indicator-border-color, transparent);
  border-radius: var(--%NS%mat-focus-indicator-border-radius, 4px);
}
.mat-focus-indicator:focus-visible::before {
  content: "";
}

@media (forced-colors: active) {
  html {
    --%NS%mat-focus-indicator-display: block;
    --%NS%mat-focus-indicator-fallback-border-style: none;
  }
}
`],encapsulation:2})}return t})();var zI=new y("MAT_BUTTON_CONFIG");function my(t){return t==null?void 0:ht(t)}var Jc=(()=>{class t{_elementRef=m(L);_ngZone=m(N);_animationsDisabled=Re();_config=m(zI,{optional:!0});_focusMonitor=m(on);_cleanupClick;_renderer=m(Se);_rippleLoader=m(Xc);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}showProgress=Vu(!1,{transform:I});constructor(){m(je).load(Ct);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",i){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,hostAttrs:[1,"mat-mdc-button-base"],hostVars:15,hostBindings:function(i,a){i&2&&(K("disabled",a._getDisabledAttribute())("aria-disabled",a._getAriaDisabled())("tabindex",a._getTabIndex()),xt(a.color?"mat-"+a.color:""),S("mat-mdc-button-progress-indicator-shown",a.showProgress())("mat-mdc-button-disabled",a.disabled)("mat-mdc-button-disabled-interactive",a.disabledInteractive)("mat-unthemed",!a.color)("_mat-animation-noopable",a._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",I],disabled:[2,"disabled","disabled",I],ariaDisabled:[2,"aria-disabled","ariaDisabled",I],disabledInteractive:[2,"disabledInteractive","disabledInteractive",I],tabIndex:[2,"tabIndex","tabIndex",my],_tabindex:[2,"tabindex","_tabindex",my],showProgress:[1,"showProgress"]}})}return t})(),Sp=(()=>{class t extends Jc{constructor(){super(),this._rippleLoader.configureRipple(this._elementRef.nativeElement,{centered:!0})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){let e=["*",[["","progressIndicator",""]]],i=["*","[progressIndicator]"];function a(r,o){r&1&&(We(0,"div",1),V(1,1),qe())}return B({type:t,selectors:[["button","mat-icon-button",""],["a","mat-icon-button",""],["button","matIconButton",""],["a","matIconButton",""]],hostAttrs:[1,"mdc-icon-button","mat-mdc-icon-button"],exportAs:["matButton","matAnchor"],features:[le],ngContentSelectors:i,decls:5,vars:1,consts:[[1,"mat-mdc-button-persistent-ripple","mdc-icon-button__ripple"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(o,s){o&1&&(he(e),it(0,"span",0),V(1),te(2,a,2,0,"div",1),it(3,"span",2)(4,"span",3)),o&2&&(v(2),ne(s.showProgress()?2:-1))},styles:[`.mat-mdc-icon-button {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  border: none;
  outline: none;
  background-color: transparent;
  fill: currentColor;
  text-decoration: none;
  cursor: pointer;
  z-index: 0;
  overflow: visible;
  border-radius: var(--%NS%mat-icon-button-container-shape, var(--%NS%mat-sys-corner-full, 50%));
  flex-shrink: 0;
  text-align: center;
  width: var(--%NS%mat-icon-button-state-layer-size, 40px);
  height: var(--%NS%mat-icon-button-state-layer-size, 40px);
  padding: calc(calc(var(--%NS%mat-icon-button-state-layer-size, 40px) - var(--%NS%mat-icon-button-icon-size, 24px)) / 2);
  font-size: var(--%NS%mat-icon-button-icon-size, 24px);
  color: var(--%NS%mat-icon-button-icon-color, var(--%NS%mat-sys-on-surface-variant));
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-icon-button .mat-mdc-button-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-icon-button .mdc-button__label,
.mat-mdc-icon-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-icon-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-icon-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-ripple-element {
  background-color: var(--%NS%mat-icon-button-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface-variant) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-icon-button-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-icon-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-icon-button-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-icon-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-icon-button-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-icon-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-icon-button-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-icon-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-icon-button-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-icon-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-icon-button-touch-target-size, 48px);
  display: var(--%NS%mat-icon-button-touch-target-display, block);
  left: 50%;
  width: var(--%NS%mat-icon-button-touch-target-size, 48px);
  transform: translate(-50%, -50%);
}
.mat-mdc-icon-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-icon-button[disabled], .mat-mdc-icon-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-icon-button-disabled-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-icon-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-icon-button img,
.mat-mdc-icon-button svg {
  width: var(--%NS%mat-icon-button-icon-size, 24px);
  height: var(--%NS%mat-icon-button-icon-size, 24px);
  vertical-align: baseline;
}
.mat-mdc-icon-button .mat-mdc-button-progress-indicator-container .mdc-circular-progress__determinate-circle-graphic {
  width: inherit;
  height: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-progress-indicator-container .mdc-circular-progress__indeterminate-circle-graphic {
  height: 100%;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple {
  border-radius: var(--%NS%mat-icon-button-container-shape, var(--%NS%mat-sys-corner-full, 50%));
}
.mat-mdc-icon-button[hidden] {
  display: none;
}
.mat-mdc-icon-button.mat-unthemed:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-primary:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-accent:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-warn:not(.mdc-ripple-upgraded):focus::before {
  background: transparent;
  opacity: 1;
}

.mat-mdc-button-progress-indicator-container {
  position: absolute;
  inset-inline-start: 0;
  inset-block-start: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  box-sizing: border-box;
}

.mat-mdc-button-progress-indicator-shown mat-icon {
  visibility: hidden;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2})})()}return t})();var HI=new y("cdk-dir-doc",{providedIn:"root",factory:()=>m(ee)}),UI=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function uy(t){let n=t?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?UI.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var Mt=(()=>{class t{get value(){return this.valueSignal()}valueSignal=ae("ltr");change=new z;constructor(){let e=m(HI,{optional:!0});if(e){let i=e.body?e.body.dir:null,a=e.documentElement?e.documentElement.dir:null;this.valueSignal.set(uy(i||a||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})}return t})();var me=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=G({})}return t})();var Sn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=G({imports:[me]})}return t})();var py=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),el=(()=>{class t extends Jc{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=$I(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let i=this._elementRef.nativeElement.classList,a=this._appearance?py.get(this._appearance):null,r=py.get(e);a&&i.remove(...a),i.add(...r),this._appearance=e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){let e=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""],["",8,"material-symbols-outlined",3,"iconPositionEnd",""],["",8,"material-symbols-rounded",3,"iconPositionEnd",""],["",8,"material-symbols-sharp",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""],["","iconPositionEnd","",8,"material-symbols-outlined"],["","iconPositionEnd","",8,"material-symbols-rounded"],["","iconPositionEnd","",8,"material-symbols-sharp"]],[["","progressIndicator",""]]],i=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd]), .material-symbols-outlined:not([iconPositionEnd]), .material-symbols-rounded:not([iconPositionEnd]), .material-symbols-sharp:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd], .material-symbols-outlined[iconPositionEnd], .material-symbols-rounded[iconPositionEnd], .material-symbols-sharp[iconPositionEnd]","[progressIndicator]"];function a(r,o){r&1&&(We(0,"div",2),V(1,3),qe())}return B({type:t,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[le],ngContentSelectors:i,decls:8,vars:5,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(o,s){o&1&&(he(e),it(0,"span",0),V(1),We(2,"span",1),V(3,1),qe(),V(4,2),te(5,a,2,0,"div",2),it(6,"span",3)(7,"span",4)),o&2&&(S("mdc-button__ripple",!s._isFab)("mdc-fab__ripple",s._isFab),v(5),ne(s.showProgress()?5:-1))},styles:[`.mat-mdc-button-base {
  text-decoration: none;
}
.mat-mdc-button-base .mat-icon {
  min-height: fit-content;
  flex-shrink: 0;
}
@media (hover: none) {
  .mat-mdc-button-base:hover > span.mat-mdc-button-persistent-ripple::before {
    opacity: 0;
  }
}

.mdc-button {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  min-width: 64px;
  border: none;
  outline: none;
  line-height: inherit;
  -webkit-appearance: none;
  overflow: visible;
  vertical-align: middle;
  background: transparent;
  padding: 0 8px;
}
.mdc-button::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mdc-button:active {
  outline: none;
}
.mdc-button:hover {
  cursor: pointer;
}
.mdc-button:disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-button[hidden] {
  display: none;
}
.mdc-button .mdc-button__label {
  position: relative;
}

.mat-mdc-button {
  padding: 0 var(--%NS%mat-button-text-horizontal-padding, 12px);
  height: var(--%NS%mat-button-text-container-height, 40px);
  font-family: var(--%NS%mat-button-text-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-text-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-text-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-text-label-text-transform);
  font-weight: var(--%NS%mat-button-text-label-text-weight, var(--%NS%mat-sys-label-large-weight));
}
.mat-mdc-button, .mat-mdc-button .mdc-button__ripple {
  border-radius: var(--%NS%mat-button-text-container-shape, var(--%NS%mat-sys-corner-full));
}
.mat-mdc-button:not(:disabled) {
  color: var(--%NS%mat-button-text-label-text-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-button[disabled], .mat-mdc-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-text-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-button:has(.material-icons, .material-symbols-outlined, .material-symbols-rounded,
.material-symbols-sharp, mat-icon, [matButtonIcon]) {
  padding: 0 var(--%NS%mat-button-text-with-icon-horizontal-padding, 16px);
}
.mat-mdc-button > .mat-icon {
  margin-right: var(--%NS%mat-button-text-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-text-icon-offset, -4px);
}
[dir=rtl] .mat-mdc-button > .mat-icon {
  margin-right: var(--%NS%mat-button-text-icon-offset, -4px);
  margin-left: var(--%NS%mat-button-text-icon-spacing, 8px);
}
.mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-text-icon-offset, -4px);
  margin-left: var(--%NS%mat-button-text-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-text-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-text-icon-offset, -4px);
}
.mat-mdc-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-text-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-primary) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-text-state-layer-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-text-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-text-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-text-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-text-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-text-touch-target-size, 48px);
  display: var(--%NS%mat-button-text-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-unelevated-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--%NS%mat-button-filled-container-height, 40px);
  font-family: var(--%NS%mat-button-filled-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-filled-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-filled-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-filled-label-text-transform);
  font-weight: var(--%NS%mat-button-filled-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  padding: 0 var(--%NS%mat-button-filled-horizontal-padding, 24px);
}
.mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--%NS%mat-button-filled-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-filled-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--%NS%mat-button-filled-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-filled-icon-spacing, 8px);
}
.mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-filled-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-filled-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-filled-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-filled-icon-offset, -8px);
}
.mat-mdc-unelevated-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-filled-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-primary) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-filled-state-layer-color, var(--%NS%mat-sys-on-primary));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-filled-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-unelevated-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-filled-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-unelevated-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-filled-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-unelevated-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-filled-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-unelevated-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-filled-touch-target-size, 48px);
  display: var(--%NS%mat-button-filled-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-unelevated-button:not(:disabled) {
  color: var(--%NS%mat-button-filled-label-text-color, var(--%NS%mat-sys-on-primary));
  background-color: var(--%NS%mat-button-filled-container-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-unelevated-button, .mat-mdc-unelevated-button .mdc-button__ripple {
  border-radius: var(--%NS%mat-button-filled-container-shape, var(--%NS%mat-sys-corner-full));
}
.mat-mdc-unelevated-button .mat-mdc-button-progress-indicator-container {
  --%NS%mat-progress-spinner-active-indicator-color: var(--%NS%mat-button-filled-progress-active-indicator-color, var(--%NS%mat-sys-on-primary));
}
.mat-mdc-unelevated-button[disabled], .mat-mdc-unelevated-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-filled-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-button-filled-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-raised-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: var(--%NS%mat-button-protected-container-elevation-shadow, var(--%NS%mat-sys-level1));
  height: var(--%NS%mat-button-protected-container-height, 40px);
  font-family: var(--%NS%mat-button-protected-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-protected-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-protected-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-protected-label-text-transform);
  font-weight: var(--%NS%mat-button-protected-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  padding: 0 var(--%NS%mat-button-protected-horizontal-padding, 24px);
}
.mat-mdc-raised-button > .mat-icon {
  margin-right: var(--%NS%mat-button-protected-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-protected-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-raised-button > .mat-icon {
  margin-right: var(--%NS%mat-button-protected-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-protected-icon-spacing, 8px);
}
.mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-protected-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-protected-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-protected-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-protected-icon-offset, -8px);
}
.mat-mdc-raised-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-protected-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-primary) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-protected-state-layer-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-raised-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-protected-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-raised-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-protected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-raised-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-protected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-raised-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-protected-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-raised-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-protected-touch-target-size, 48px);
  display: var(--%NS%mat-button-protected-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-raised-button:not(:disabled) {
  color: var(--%NS%mat-button-protected-label-text-color, var(--%NS%mat-sys-primary));
  background-color: var(--%NS%mat-button-protected-container-color, var(--%NS%mat-sys-surface));
}
.mat-mdc-raised-button, .mat-mdc-raised-button .mdc-button__ripple {
  border-radius: var(--%NS%mat-button-protected-container-shape, var(--%NS%mat-sys-corner-full));
}
@media (hover: hover) {
  .mat-mdc-raised-button:hover {
    box-shadow: var(--%NS%mat-button-protected-hover-container-elevation-shadow, var(--%NS%mat-sys-level2));
  }
}
.mat-mdc-raised-button:focus {
  box-shadow: var(--%NS%mat-button-protected-focus-container-elevation-shadow, var(--%NS%mat-sys-level1));
}
.mat-mdc-raised-button:active, .mat-mdc-raised-button:focus:active {
  box-shadow: var(--%NS%mat-button-protected-pressed-container-elevation-shadow, var(--%NS%mat-sys-level1));
}
.mat-mdc-raised-button[disabled], .mat-mdc-raised-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-protected-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-button-protected-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-raised-button[disabled].mat-mdc-button-disabled, .mat-mdc-raised-button.mat-mdc-button-disabled.mat-mdc-button-disabled {
  box-shadow: var(--%NS%mat-button-protected-disabled-container-elevation-shadow, var(--%NS%mat-sys-level0));
}
.mat-mdc-raised-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-outlined-button {
  border-style: solid;
  transition: border 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--%NS%mat-button-outlined-container-height, 40px);
  font-family: var(--%NS%mat-button-outlined-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-outlined-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-outlined-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-outlined-label-text-transform);
  font-weight: var(--%NS%mat-button-outlined-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  border-radius: var(--%NS%mat-button-outlined-container-shape, var(--%NS%mat-sys-corner-full));
  border-width: var(--%NS%mat-button-outlined-outline-width, 1px);
  padding: 0 var(--%NS%mat-button-outlined-horizontal-padding, 24px);
}
.mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--%NS%mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-outlined-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--%NS%mat-button-outlined-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-outlined-icon-spacing, 8px);
}
.mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-outlined-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-outlined-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-outlined-icon-offset, -8px);
}
.mat-mdc-outlined-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-outlined-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-primary) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-outlined-state-layer-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-outlined-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-outlined-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-outlined-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-outlined-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-outlined-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-outlined-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-outlined-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-outlined-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-outlined-touch-target-size, 48px);
  display: var(--%NS%mat-button-outlined-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-outlined-button:not(:disabled) {
  color: var(--%NS%mat-button-outlined-label-text-color, var(--%NS%mat-sys-primary));
  border-color: var(--%NS%mat-button-outlined-outline-color, var(--%NS%mat-sys-outline));
}
.mat-mdc-outlined-button[disabled], .mat-mdc-outlined-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-outlined-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  border-color: var(--%NS%mat-button-outlined-disabled-outline-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-tonal-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--%NS%mat-button-tonal-container-height, 40px);
  font-family: var(--%NS%mat-button-tonal-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-tonal-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-tonal-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-tonal-label-text-transform);
  font-weight: var(--%NS%mat-button-tonal-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  padding: 0 var(--%NS%mat-button-tonal-horizontal-padding, 24px);
}
.mat-tonal-button:not(:disabled) {
  color: var(--%NS%mat-button-tonal-label-text-color, var(--%NS%mat-sys-on-secondary-container));
  background-color: var(--%NS%mat-button-tonal-container-color, var(--%NS%mat-sys-secondary-container));
}
.mat-tonal-button, .mat-tonal-button .mdc-button__ripple {
  border-radius: var(--%NS%mat-button-tonal-container-shape, var(--%NS%mat-sys-corner-full));
}
.mat-tonal-button[disabled], .mat-tonal-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-tonal-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-button-tonal-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-tonal-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-tonal-button > .mat-icon {
  margin-right: var(--%NS%mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-tonal-icon-offset, -8px);
}
[dir=rtl] .mat-tonal-button > .mat-icon {
  margin-right: var(--%NS%mat-button-tonal-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-tonal-icon-spacing, 8px);
}
.mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-tonal-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-tonal-icon-spacing, 8px);
}
[dir=rtl] .mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-tonal-icon-offset, -8px);
}
.mat-tonal-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-tonal-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-secondary-container) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-tonal-state-layer-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-tonal-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-tonal-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-tonal-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-tonal-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-tonal-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-tonal-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-tonal-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-tonal-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-tonal-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-tonal-touch-target-size, 48px);
  display: var(--%NS%mat-button-tonal-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-button,
.mat-mdc-unelevated-button,
.mat-mdc-raised-button,
.mat-mdc-outlined-button,
.mat-tonal-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-button .mdc-button__label,
.mat-mdc-button .mat-icon,
.mat-mdc-unelevated-button .mdc-button__label,
.mat-mdc-unelevated-button .mat-icon,
.mat-mdc-raised-button .mdc-button__label,
.mat-mdc-raised-button .mat-icon,
.mat-mdc-outlined-button .mdc-button__label,
.mat-mdc-outlined-button .mat-icon,
.mat-tonal-button .mdc-button__label,
.mat-tonal-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-button .mat-focus-indicator,
.mat-mdc-unelevated-button .mat-focus-indicator,
.mat-mdc-raised-button .mat-focus-indicator,
.mat-mdc-outlined-button .mat-focus-indicator,
.mat-tonal-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-unelevated-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-raised-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-outlined-button:focus-visible > .mat-focus-indicator::before,
.mat-tonal-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-button._mat-animation-noopable,
.mat-mdc-unelevated-button._mat-animation-noopable,
.mat-mdc-raised-button._mat-animation-noopable,
.mat-mdc-outlined-button._mat-animation-noopable,
.mat-tonal-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-button > .mat-icon,
.mat-mdc-unelevated-button > .mat-icon,
.mat-mdc-raised-button > .mat-icon,
.mat-mdc-outlined-button > .mat-icon,
.mat-tonal-button > .mat-icon {
  display: inline-block;
  position: relative;
  vertical-align: top;
  font-size: 1.125rem;
  height: 1.125rem;
  width: 1.125rem;
}

.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mdc-button__ripple {
  top: -1px;
  left: -1px;
  bottom: -1px;
  right: -1px;
}

.mat-mdc-unelevated-button .mat-focus-indicator::before,
.mat-tonal-button .mat-focus-indicator::before,
.mat-mdc-raised-button .mat-focus-indicator::before {
  margin: calc(calc(var(--%NS%mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-outlined-button .mat-focus-indicator::before {
  margin: calc(calc(var(--%NS%mat-focus-indicator-border-width, 3px) + 3px) * -1);
}

.mat-mdc-button-progress-indicator-container {
  position: absolute;
  inset-inline-start: 0;
  inset-block-start: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  box-sizing: border-box;
}

.mat-mdc-button-progress-indicator-shown mat-icon,
.mat-mdc-button-progress-indicator-shown [matButtonIcon],
.mat-mdc-button-progress-indicator-shown .mdc-button__label {
  visibility: hidden;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2})})()}return t})();function $I(t){return t.hasAttribute("mat-raised-button")?"elevated":t.hasAttribute("mat-stroked-button")?"outlined":t.hasAttribute("mat-flat-button")?"filled":t.hasAttribute("mat-button")?"text":null}var GI=new y("mat-mdc-fab-default-options",{providedIn:"root",factory:()=>Ep}),Ep={color:"accent"},hy=(()=>{class t extends Jc{_options=m(GI,{optional:!0});_isFab=!0;extended=!1;constructor(){super(),this._options=this._options||Ep,this.color=this._options.color||Ep.color}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){let e=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""],["",8,"material-symbols-outlined",3,"iconPositionEnd",""],["",8,"material-symbols-rounded",3,"iconPositionEnd",""],["",8,"material-symbols-sharp",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""],["","iconPositionEnd","",8,"material-symbols-outlined"],["","iconPositionEnd","",8,"material-symbols-rounded"],["","iconPositionEnd","",8,"material-symbols-sharp"]],[["","progressIndicator",""]]],i=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd]), .material-symbols-outlined:not([iconPositionEnd]), .material-symbols-rounded:not([iconPositionEnd]), .material-symbols-sharp:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd], .material-symbols-outlined[iconPositionEnd], .material-symbols-rounded[iconPositionEnd], .material-symbols-sharp[iconPositionEnd]","[progressIndicator]"];function a(r,o){r&1&&(We(0,"div",2),V(1,3),qe())}return B({type:t,selectors:[["button","mat-fab",""],["a","mat-fab",""],["button","matFab",""],["a","matFab",""]],hostAttrs:[1,"mdc-fab","mat-mdc-fab-base","mat-mdc-fab"],hostVars:4,hostBindings:function(o,s){o&2&&S("mdc-fab--extended",s.extended)("mat-mdc-extended-fab",s.extended)},inputs:{extended:[2,"extended","extended",I]},exportAs:["matButton","matAnchor"],features:[le],ngContentSelectors:i,decls:8,vars:5,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(o,s){o&1&&(he(e),it(0,"span",0),V(1),We(2,"span",1),V(3,1),qe(),V(4,2),te(5,a,2,0,"div",2),it(6,"span",3)(7,"span",4)),o&2&&(S("mdc-button__ripple",!s._isFab)("mdc-fab__ripple",s._isFab),v(5),ne(s.showProgress()?5:-1))},styles:[`.mat-mdc-fab-base {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  width: 56px;
  height: 56px;
  padding: 0;
  border: none;
  fill: currentColor;
  text-decoration: none;
  cursor: pointer;
  -moz-appearance: none;
  -webkit-appearance: none;
  overflow: visible;
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1), opacity 15ms linear 30ms, transform 270ms 0ms cubic-bezier(0, 0, 0.2, 1);
  flex-shrink: 0;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-fab-base .mat-mdc-button-ripple,
.mat-mdc-fab-base .mat-mdc-button-persistent-ripple,
.mat-mdc-fab-base .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-fab-base .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-fab-base .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-fab-base .mdc-button__label,
.mat-mdc-fab-base .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-fab-base .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
}
.mat-mdc-fab-base:focus-visible > .mat-focus-indicator::before {
  content: "";
}
.mat-mdc-fab-base._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-fab-base::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  border: 1px solid transparent;
  border-radius: inherit;
  content: "";
  pointer-events: none;
}
.mat-mdc-fab-base[hidden] {
  display: none;
}
.mat-mdc-fab-base::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mat-mdc-fab-base:active, .mat-mdc-fab-base:focus {
  outline: none;
}
.mat-mdc-fab-base:hover {
  cursor: pointer;
}
.mat-mdc-fab-base > svg {
  width: 100%;
}
.mat-mdc-fab-base .mat-icon,
.mat-mdc-fab-base .material-icons,
.mat-mdc-fab-base .material-symbols-outlined,
.mat-mdc-fab-base .material-symbols-rounded,
.mat-mdc-fab-base .material-symbols-sharp {
  transition: transform 180ms 90ms cubic-bezier(0, 0, 0.2, 1);
  fill: currentColor;
  will-change: transform;
}
.mat-mdc-fab-base .mat-focus-indicator::before {
  margin: calc(calc(var(--%NS%mat-focus-indicator-border-width, 3px) + 2px) * -1);
  border-radius: calc(var(--%NS%mat-fab-container-shape, var(--%NS%mat-sys-corner-large)) + calc(var(--%NS%mat-focus-indicator-border-width, 3px) + 2px));
}
.mat-mdc-fab-base[disabled], .mat-mdc-fab-base.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-fab-base[disabled], .mat-mdc-fab-base[disabled]:focus, .mat-mdc-fab-base.mat-mdc-button-disabled, .mat-mdc-fab-base.mat-mdc-button-disabled:focus {
  box-shadow: none;
}
.mat-mdc-fab-base.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-fab {
  background-color: var(--%NS%mat-fab-container-color, var(--%NS%mat-sys-primary-container));
  border-radius: var(--%NS%mat-fab-container-shape, var(--%NS%mat-sys-corner-large));
  color: var(--%NS%mat-fab-foreground-color, var(--%NS%mat-sys-on-primary-container, inherit));
  box-shadow: var(--%NS%mat-fab-container-elevation-shadow, var(--%NS%mat-sys-level3));
}
@media (hover: hover) {
  .mat-mdc-fab:hover {
    box-shadow: var(--%NS%mat-fab-hover-container-elevation-shadow, var(--%NS%mat-sys-level4));
  }
}
.mat-mdc-fab:focus {
  box-shadow: var(--%NS%mat-fab-focus-container-elevation-shadow, var(--%NS%mat-sys-level3));
}
.mat-mdc-fab:active, .mat-mdc-fab:focus:active {
  box-shadow: var(--%NS%mat-fab-pressed-container-elevation-shadow, var(--%NS%mat-sys-level3));
}
.mat-mdc-fab[disabled], .mat-mdc-fab.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-fab-disabled-state-foreground-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-fab-disabled-state-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-fab.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-fab .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-fab-touch-target-size, 48px);
  display: var(--%NS%mat-fab-touch-target-display, block);
  left: 50%;
  width: var(--%NS%mat-fab-touch-target-size, 48px);
  transform: translate(-50%, -50%);
}
.mat-mdc-fab .mat-ripple-element {
  background-color: var(--%NS%mat-fab-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-primary-container) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-fab .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-fab-state-layer-color, var(--%NS%mat-sys-on-primary-container));
}
.mat-mdc-fab.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-fab-disabled-state-layer-color);
}
.mat-mdc-fab:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-fab-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-fab.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-fab.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-fab.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-fab-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-fab:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-fab-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}

.mat-mdc-mini-fab {
  width: 40px;
  height: 40px;
  background-color: var(--%NS%mat-fab-small-container-color, var(--%NS%mat-sys-primary-container));
  border-radius: var(--%NS%mat-fab-small-container-shape, var(--%NS%mat-sys-corner-medium));
  color: var(--%NS%mat-fab-small-foreground-color, var(--%NS%mat-sys-on-primary-container, inherit));
  box-shadow: var(--%NS%mat-fab-small-container-elevation-shadow, var(--%NS%mat-sys-level3));
}
@media (hover: hover) {
  .mat-mdc-mini-fab:hover {
    box-shadow: var(--%NS%mat-fab-small-hover-container-elevation-shadow, var(--%NS%mat-sys-level4));
  }
}
.mat-mdc-mini-fab:focus {
  box-shadow: var(--%NS%mat-fab-small-focus-container-elevation-shadow, var(--%NS%mat-sys-level3));
}
.mat-mdc-mini-fab:active, .mat-mdc-mini-fab:focus:active {
  box-shadow: var(--%NS%mat-fab-small-pressed-container-elevation-shadow, var(--%NS%mat-sys-level3));
}
.mat-mdc-mini-fab .mat-focus-indicator::before {
  border-radius: calc(var(--%NS%mat-fab-small-container-shape, var(--%NS%mat-sys-corner-medium)) + calc(var(--%NS%mat-focus-indicator-border-width, 3px) + 2px));
}
.mat-mdc-mini-fab[disabled], .mat-mdc-mini-fab.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-fab-small-disabled-state-foreground-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-fab-small-disabled-state-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-mini-fab.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-mini-fab .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-fab-small-touch-target-size, 48px);
  display: var(--%NS%mat-fab-small-touch-target-display);
  left: 50%;
  width: var(--%NS%mat-fab-small-touch-target-size, 48px);
  transform: translate(-50%, -50%);
}
.mat-mdc-mini-fab .mat-ripple-element {
  background-color: var(--%NS%mat-fab-small-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-primary-container) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-mini-fab .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-fab-small-state-layer-color, var(--%NS%mat-sys-on-primary-container));
}
.mat-mdc-mini-fab.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-fab-small-disabled-state-layer-color);
}
.mat-mdc-mini-fab:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-fab-small-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-mini-fab.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-mini-fab.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-mini-fab.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-fab-small-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-mini-fab:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-fab-small-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}

.mat-mdc-extended-fab {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  padding-left: 20px;
  padding-right: 20px;
  width: auto;
  max-width: 100%;
  line-height: normal;
  box-shadow: var(--%NS%mat-fab-extended-container-elevation-shadow, var(--%NS%mat-sys-level3));
  height: var(--%NS%mat-fab-extended-container-height, 56px);
  border-radius: var(--%NS%mat-fab-extended-container-shape, var(--%NS%mat-sys-corner-large));
  font-family: var(--%NS%mat-fab-extended-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-fab-extended-label-text-size, var(--%NS%mat-sys-label-large-size));
  font-weight: var(--%NS%mat-fab-extended-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  letter-spacing: var(--%NS%mat-fab-extended-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
}
@media (hover: hover) {
  .mat-mdc-extended-fab:hover {
    box-shadow: var(--%NS%mat-fab-extended-hover-container-elevation-shadow, var(--%NS%mat-sys-level4));
  }
}
.mat-mdc-extended-fab:focus {
  box-shadow: var(--%NS%mat-fab-extended-focus-container-elevation-shadow, var(--%NS%mat-sys-level3));
}
.mat-mdc-extended-fab:active, .mat-mdc-extended-fab:focus:active {
  box-shadow: var(--%NS%mat-fab-extended-pressed-container-elevation-shadow, var(--%NS%mat-sys-level3));
}
.mat-mdc-extended-fab[disabled], .mat-mdc-extended-fab.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-extended-fab[disabled], .mat-mdc-extended-fab[disabled]:focus, .mat-mdc-extended-fab.mat-mdc-button-disabled, .mat-mdc-extended-fab.mat-mdc-button-disabled:focus {
  box-shadow: none;
}
.mat-mdc-extended-fab.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
[dir=rtl] .mat-mdc-extended-fab .mdc-button__label + .mat-icon, [dir=rtl] .mat-mdc-extended-fab .mdc-button__label + .material-icons, [dir=rtl] .mat-mdc-extended-fab .mdc-button__label + .material-symbols-outlined, [dir=rtl] .mat-mdc-extended-fab .mdc-button__label + .material-symbols-rounded, [dir=rtl] .mat-mdc-extended-fab .mdc-button__label + .material-symbols-sharp,
.mat-mdc-extended-fab > .mat-icon,
.mat-mdc-extended-fab > .material-icons,
.mat-mdc-extended-fab > .material-symbols-outlined,
.mat-mdc-extended-fab > .material-symbols-rounded,
.mat-mdc-extended-fab > .material-symbols-sharp {
  margin-left: -8px;
  margin-right: 12px;
}
.mat-mdc-extended-fab .mdc-button__label + .mat-icon,
.mat-mdc-extended-fab .mdc-button__label + .material-icons,
.mat-mdc-extended-fab .mdc-button__label + .material-symbols-outlined,
.mat-mdc-extended-fab .mdc-button__label + .material-symbols-rounded,
.mat-mdc-extended-fab .mdc-button__label + .material-symbols-sharp, [dir=rtl] .mat-mdc-extended-fab > .mat-icon, [dir=rtl] .mat-mdc-extended-fab > .material-icons, [dir=rtl] .mat-mdc-extended-fab > .material-symbols-outlined, [dir=rtl] .mat-mdc-extended-fab > .material-symbols-rounded, [dir=rtl] .mat-mdc-extended-fab > .material-symbols-sharp {
  margin-left: 12px;
  margin-right: -8px;
}
.mat-mdc-extended-fab .mat-mdc-button-touch-target {
  width: 100%;
}

.mat-mdc-button-progress-indicator-container {
  position: absolute;
  inset-inline-start: 0;
  margin-block-start: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  box-sizing: border-box;
}

.mat-mdc-button-progress-indicator-shown mat-icon,
.mat-mdc-button-progress-indicator-shown [matButtonIcon],
.mat-mdc-button-progress-indicator-shown .mdc-button__label {
  visibility: hidden;
}
`],encapsulation:2})})()}return t})();var tl=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=G({imports:[Sn,me]})}return t})();var qI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["mat-toolbar-row"]],hostAttrs:[1,"mat-toolbar-row"],exportAs:["matToolbarRow"]})}return t})(),fy=(()=>{class t{_elementRef=m(L);_platform=m(ye);_document=m(ee);color;_toolbarRows;ngAfterViewInit(){this._platform.isBrowser&&(this._checkToolbarMixedModes(),this._toolbarRows.changes.subscribe(()=>this._checkToolbarMixedModes()))}_checkToolbarMixedModes(){this._toolbarRows.length}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){let e=["*",[["mat-toolbar-row"]]];return B({type:t,selectors:[["mat-toolbar"]],contentQueries:function(r,o,s){if(r&1&&Ye(s,qI,5),r&2){let c;M(c=T())&&(o._toolbarRows=c)}},hostAttrs:[1,"mat-toolbar"],hostVars:6,hostBindings:function(r,o){r&2&&(xt(o.color?"mat-"+o.color:""),S("mat-toolbar-multiple-rows",o._toolbarRows.length>0)("mat-toolbar-single-row",o._toolbarRows.length===0))},inputs:{color:"color"},exportAs:["matToolbar"],ngContentSelectors:["*","mat-toolbar-row"],decls:2,vars:0,template:function(r,o){r&1&&(he(e),V(0),V(1,1))},styles:[`.mat-toolbar {
  background: var(--%NS%mat-toolbar-container-background-color, var(--%NS%mat-sys-surface));
  color: var(--%NS%mat-toolbar-container-text-color, var(--%NS%mat-sys-on-surface));
}
.mat-toolbar, .mat-toolbar h1, .mat-toolbar h2, .mat-toolbar h3, .mat-toolbar h4, .mat-toolbar h5, .mat-toolbar h6 {
  font-family: var(--%NS%mat-toolbar-title-text-font, var(--%NS%mat-sys-title-large-font));
  font-size: var(--%NS%mat-toolbar-title-text-size, var(--%NS%mat-sys-title-large-size));
  line-height: var(--%NS%mat-toolbar-title-text-line-height, var(--%NS%mat-sys-title-large-line-height));
  font-weight: var(--%NS%mat-toolbar-title-text-weight, var(--%NS%mat-sys-title-large-weight));
  letter-spacing: var(--%NS%mat-toolbar-title-text-tracking, var(--%NS%mat-sys-title-large-tracking));
  margin: 0;
}
@media (forced-colors: active) {
  .mat-toolbar {
    outline: solid 1px;
  }
}
.mat-toolbar .mat-form-field-underline,
.mat-toolbar .mat-form-field-ripple,
.mat-toolbar .mat-focused .mat-form-field-ripple {
  background-color: currentColor;
}
.mat-toolbar .mat-form-field-label,
.mat-toolbar .mat-focused .mat-form-field-label,
.mat-toolbar .mat-select-value,
.mat-toolbar .mat-select-arrow,
.mat-toolbar .mat-form-field.mat-focused .mat-select-arrow {
  color: inherit;
}
.mat-toolbar .mat-input-element {
  caret-color: currentColor;
}
.mat-toolbar .mat-mdc-button-base.mat-mdc-button-base.mat-unthemed {
  --%NS%mat-button-text-label-text-color: var(--%NS%mat-toolbar-container-text-color, var(--%NS%mat-sys-on-surface));
  --%NS%mat-button-outlined-label-text-color: var(--%NS%mat-toolbar-container-text-color, var(--%NS%mat-sys-on-surface));
}

.mat-toolbar-row, .mat-toolbar-single-row {
  display: flex;
  box-sizing: border-box;
  padding: 0 16px;
  width: 100%;
  flex-direction: row;
  align-items: center;
  white-space: nowrap;
  height: var(--%NS%mat-toolbar-standard-height, 64px);
}
@media (max-width: 599px) {
  .mat-toolbar-row, .mat-toolbar-single-row {
    height: var(--%NS%mat-toolbar-mobile-height, 56px);
  }
}

.mat-toolbar-multiple-rows {
  display: flex;
  box-sizing: border-box;
  flex-direction: column;
  width: 100%;
  min-height: var(--%NS%mat-toolbar-standard-height, 64px);
}
@media (max-width: 599px) {
  .mat-toolbar-multiple-rows {
    min-height: var(--%NS%mat-toolbar-mobile-height, 56px);
  }
}
`],encapsulation:2})})()}return t})();var gy=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=G({imports:[me]})}return t})();var ZI=new y("MAT_PROGRESS_BAR_DEFAULT_OPTIONS");var vy=(()=>{class t{_elementRef=m(L);_ngZone=m(N);_changeDetectorRef=m(Ie);_renderer=m(Se);_cleanupTransitionEnd;constructor(){let e=yp(),i=m(ZI,{optional:!0});this._isNoopAnimation=e==="di-disabled",e==="reduced-motion"&&this._elementRef.nativeElement.classList.add("mat-progress-bar-reduced-motion"),i&&(i.color&&(this.color=this._defaultColor=i.color),this.mode=i.mode||this.mode)}_isNoopAnimation;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;_defaultColor="primary";get value(){return this._value}set value(e){this._value=by(e||0),this._changeDetectorRef.markForCheck()}_value=0;get bufferValue(){return this._bufferValue||0}set bufferValue(e){this._bufferValue=by(e||0),this._changeDetectorRef.markForCheck()}_bufferValue=0;animationEnd=new z;get mode(){return this._mode}set mode(e){this._mode=e,this._changeDetectorRef.markForCheck()}_mode="determinate";ngAfterViewInit(){this._ngZone.runOutsideAngular(()=>{this._cleanupTransitionEnd=this._renderer.listen(this._elementRef.nativeElement,"transitionend",this._transitionendHandler)})}ngOnDestroy(){this._cleanupTransitionEnd?.()}_getPrimaryBarTransform(){return`scaleX(${this._isIndeterminate()?1:this.value/100})`}_getBufferBarFlexBasis(){return`${this.mode==="buffer"?this.bufferValue:100}%`}_isIndeterminate(){return this.mode==="indeterminate"||this.mode==="query"}_transitionendHandler=e=>{this.animationEnd.observers.length===0||!e.target||!e.target.classList.contains("mdc-linear-progress__primary-bar")||(this.mode==="determinate"||this.mode==="buffer")&&this._ngZone.run(()=>this.animationEnd.next({value:this.value}))};static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){function e(i,a){i&1&&it(0,"div",2)}return B({type:t,selectors:[["mat-progress-bar"]],hostAttrs:["role","progressbar","aria-valuemin","0","aria-valuemax","100","tabindex","-1",1,"mat-mdc-progress-bar","mdc-linear-progress"],hostVars:10,hostBindings:function(a,r){a&2&&(K("aria-valuenow",r._isIndeterminate()?null:r.value)("mode",r.mode),xt("mat-"+r.color),S("_mat-animation-noopable",r._isNoopAnimation)("mdc-linear-progress--animation-ready",!r._isNoopAnimation)("mdc-linear-progress--indeterminate",r._isIndeterminate()))},inputs:{color:"color",value:[2,"value","value",ht],bufferValue:[2,"bufferValue","bufferValue",ht],mode:"mode"},outputs:{animationEnd:"animationEnd"},exportAs:["matProgressBar"],decls:7,vars:5,consts:[["aria-hidden","true",1,"mdc-linear-progress__buffer"],[1,"mdc-linear-progress__buffer-bar"],[1,"mdc-linear-progress__buffer-dots"],["aria-hidden","true",1,"mdc-linear-progress__bar","mdc-linear-progress__primary-bar"],[1,"mdc-linear-progress__bar-inner"],["aria-hidden","true",1,"mdc-linear-progress__bar","mdc-linear-progress__secondary-bar"]],template:function(a,r){a&1&&(We(0,"div",0),it(1,"div",1),te(2,e,1,0,"div",2),qe(),We(3,"div",3),it(4,"span",4),qe(),We(5,"div",5),it(6,"span",4),qe()),a&2&&(v(),oi("flex-basis",r._getBufferBarFlexBasis()),v(),ne(r.mode==="buffer"?2:-1),v(),oi("transform",r._getPrimaryBarTransform()))},styles:[`.mat-mdc-progress-bar {
  --%NS%mat-progress-bar-animation-multiplier: 1;
  display: block;
  text-align: start;
}
.mat-mdc-progress-bar[mode=query] {
  transform: scaleX(-1);
}
.mat-mdc-progress-bar._mat-animation-noopable .mdc-linear-progress__buffer-dots,
.mat-mdc-progress-bar._mat-animation-noopable .mdc-linear-progress__primary-bar,
.mat-mdc-progress-bar._mat-animation-noopable .mdc-linear-progress__secondary-bar,
.mat-mdc-progress-bar._mat-animation-noopable .mdc-linear-progress__bar-inner.mdc-linear-progress__bar-inner {
  animation: none;
}
.mat-mdc-progress-bar._mat-animation-noopable .mdc-linear-progress__primary-bar,
.mat-mdc-progress-bar._mat-animation-noopable .mdc-linear-progress__buffer-bar {
  transition: transform 1ms;
}

.mat-progress-bar-reduced-motion {
  --%NS%mat-progress-bar-animation-multiplier: 2;
}

.mdc-linear-progress {
  position: relative;
  width: 100%;
  transform: translateZ(0);
  outline: 1px solid transparent;
  overflow-x: hidden;
  transition: opacity 250ms 0ms cubic-bezier(0.4, 0, 0.6, 1);
  height: max(var(--%NS%mat-progress-bar-track-height, 4px), var(--%NS%mat-progress-bar-active-indicator-height, 4px));
}
@media (forced-colors: active) {
  .mdc-linear-progress {
    outline-color: CanvasText;
  }
}

.mdc-linear-progress__bar {
  position: absolute;
  top: 0;
  bottom: 0;
  margin: auto 0;
  width: 100%;
  animation: none;
  transform-origin: top left;
  transition: transform 250ms 0ms cubic-bezier(0.4, 0, 0.6, 1);
  height: var(--%NS%mat-progress-bar-active-indicator-height, 4px);
}
.mdc-linear-progress--indeterminate .mdc-linear-progress__bar {
  transition: none;
}
[dir=rtl] .mdc-linear-progress__bar {
  right: 0;
  transform-origin: center right;
}

.mdc-linear-progress__bar-inner {
  display: inline-block;
  position: absolute;
  width: 100%;
  animation: none;
  border-top-style: solid;
  border-color: var(--%NS%mat-progress-bar-active-indicator-color, var(--%NS%mat-sys-primary));
  border-top-width: var(--%NS%mat-progress-bar-active-indicator-height, 4px);
}

.mdc-linear-progress__buffer {
  display: flex;
  position: absolute;
  top: 0;
  bottom: 0;
  margin: auto 0;
  width: 100%;
  overflow: hidden;
  height: var(--%NS%mat-progress-bar-track-height, 4px);
  border-radius: var(--%NS%mat-progress-bar-track-shape, var(--%NS%mat-sys-corner-none));
}

.mdc-linear-progress__buffer-dots {
  background-image: radial-gradient(circle, var(--%NS%mat-progress-bar-track-color, var(--%NS%mat-sys-surface-variant)) calc(var(--%NS%mat-progress-bar-track-height, 4px) / 2), transparent 0);
  background-repeat: repeat-x;
  background-size: calc(calc(var(--%NS%mat-progress-bar-track-height, 4px) / 2) * 5);
  background-position: left;
  flex: auto;
  transform: rotate(180deg);
  animation: mdc-linear-progress-buffering calc(250ms * var(--%NS%mat-progress-bar-animation-multiplier)) infinite linear;
}
@media (forced-colors: active) {
  .mdc-linear-progress__buffer-dots {
    background-color: ButtonBorder;
  }
}
[dir=rtl] .mdc-linear-progress__buffer-dots {
  animation: mdc-linear-progress-buffering-reverse calc(250ms * var(--%NS%mat-progress-bar-animation-multiplier)) infinite linear;
  transform: rotate(0);
}

.mdc-linear-progress__buffer-bar {
  flex: 0 1 100%;
  transition: flex-basis 250ms 0ms cubic-bezier(0.4, 0, 0.6, 1);
  background-color: var(--%NS%mat-progress-bar-track-color, var(--%NS%mat-sys-surface-variant));
}

.mdc-linear-progress__primary-bar {
  transform: scaleX(0);
}
.mdc-linear-progress--indeterminate .mdc-linear-progress__primary-bar {
  left: -145.166611%;
}
.mdc-linear-progress--indeterminate.mdc-linear-progress--animation-ready .mdc-linear-progress__primary-bar {
  animation: mdc-linear-progress-primary-indeterminate-translate calc(2s * var(--%NS%mat-progress-bar-animation-multiplier)) infinite linear;
}
.mdc-linear-progress--indeterminate.mdc-linear-progress--animation-ready .mdc-linear-progress__primary-bar > .mdc-linear-progress__bar-inner {
  animation: mdc-linear-progress-primary-indeterminate-scale calc(2s * var(--%NS%mat-progress-bar-animation-multiplier)) infinite linear;
}
[dir=rtl] .mdc-linear-progress.mdc-linear-progress--animation-ready .mdc-linear-progress__primary-bar {
  animation-name: mdc-linear-progress-primary-indeterminate-translate-reverse;
}
[dir=rtl] .mdc-linear-progress.mdc-linear-progress--indeterminate .mdc-linear-progress__primary-bar {
  right: -145.166611%;
  left: auto;
}

.mdc-linear-progress__secondary-bar {
  display: none;
}
.mdc-linear-progress--indeterminate .mdc-linear-progress__secondary-bar {
  left: -54.888891%;
  display: block;
}
.mdc-linear-progress--indeterminate.mdc-linear-progress--animation-ready .mdc-linear-progress__secondary-bar {
  animation: mdc-linear-progress-secondary-indeterminate-translate calc(2s * var(--%NS%mat-progress-bar-animation-multiplier)) infinite linear;
}
.mdc-linear-progress--indeterminate.mdc-linear-progress--animation-ready .mdc-linear-progress__secondary-bar > .mdc-linear-progress__bar-inner {
  animation: mdc-linear-progress-secondary-indeterminate-scale calc(2s * var(--%NS%mat-progress-bar-animation-multiplier)) infinite linear;
}
[dir=rtl] .mdc-linear-progress.mdc-linear-progress--animation-ready .mdc-linear-progress__secondary-bar {
  animation-name: mdc-linear-progress-secondary-indeterminate-translate-reverse;
}
[dir=rtl] .mdc-linear-progress.mdc-linear-progress--indeterminate .mdc-linear-progress__secondary-bar {
  right: -54.888891%;
  left: auto;
}

@keyframes mdc-linear-progress-buffering {
  from {
    transform: rotate(180deg) translateX(calc(var(--%NS%mat-progress-bar-track-height, 4px) * -2.5));
  }
}
@keyframes mdc-linear-progress-primary-indeterminate-translate {
  0% {
    transform: translateX(0);
  }
  20% {
    animation-timing-function: cubic-bezier(0.5, 0, 0.701732, 0.495819);
    transform: translateX(0);
  }
  59.15% {
    animation-timing-function: cubic-bezier(0.302435, 0.381352, 0.55, 0.956352);
    transform: translateX(83.67142%);
  }
  100% {
    transform: translateX(200.611057%);
  }
}
@keyframes mdc-linear-progress-primary-indeterminate-scale {
  0% {
    transform: scaleX(0.08);
  }
  36.65% {
    animation-timing-function: cubic-bezier(0.334731, 0.12482, 0.785844, 1);
    transform: scaleX(0.08);
  }
  69.15% {
    animation-timing-function: cubic-bezier(0.06, 0.11, 0.6, 1);
    transform: scaleX(0.661479);
  }
  100% {
    transform: scaleX(0.08);
  }
}
@keyframes mdc-linear-progress-secondary-indeterminate-translate {
  0% {
    animation-timing-function: cubic-bezier(0.15, 0, 0.515058, 0.409685);
    transform: translateX(0);
  }
  25% {
    animation-timing-function: cubic-bezier(0.31033, 0.284058, 0.8, 0.733712);
    transform: translateX(37.651913%);
  }
  48.35% {
    animation-timing-function: cubic-bezier(0.4, 0.627035, 0.6, 0.902026);
    transform: translateX(84.386165%);
  }
  100% {
    transform: translateX(160.277782%);
  }
}
@keyframes mdc-linear-progress-secondary-indeterminate-scale {
  0% {
    animation-timing-function: cubic-bezier(0.205028, 0.057051, 0.57661, 0.453971);
    transform: scaleX(0.08);
  }
  19.15% {
    animation-timing-function: cubic-bezier(0.152313, 0.196432, 0.648374, 1.004315);
    transform: scaleX(0.457104);
  }
  44.15% {
    animation-timing-function: cubic-bezier(0.257759, -0.003163, 0.211762, 1.38179);
    transform: scaleX(0.72796);
  }
  100% {
    transform: scaleX(0.08);
  }
}
@keyframes mdc-linear-progress-primary-indeterminate-translate-reverse {
  0% {
    transform: translateX(0);
  }
  20% {
    animation-timing-function: cubic-bezier(0.5, 0, 0.701732, 0.495819);
    transform: translateX(0);
  }
  59.15% {
    animation-timing-function: cubic-bezier(0.302435, 0.381352, 0.55, 0.956352);
    transform: translateX(-83.67142%);
  }
  100% {
    transform: translateX(-200.611057%);
  }
}
@keyframes mdc-linear-progress-secondary-indeterminate-translate-reverse {
  0% {
    animation-timing-function: cubic-bezier(0.15, 0, 0.515058, 0.409685);
    transform: translateX(0);
  }
  25% {
    animation-timing-function: cubic-bezier(0.31033, 0.284058, 0.8, 0.733712);
    transform: translateX(-37.651913%);
  }
  48.35% {
    animation-timing-function: cubic-bezier(0.4, 0.627035, 0.6, 0.902026);
    transform: translateX(-84.386165%);
  }
  100% {
    transform: translateX(-160.277782%);
  }
}
@keyframes mdc-linear-progress-buffering-reverse {
  from {
    transform: translateX(-10px);
  }
}
`],encapsulation:2})})()}return t})();function by(t,n=0,e=100){return Math.max(n,Math.min(e,t))}var _y=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=G({imports:[me]})}return t})();var xy=Symbol("FIELD_TREE");var wy=Symbol("IS_ASYNC_VALIDATION_RESOURCE"),yy=class{reducer;create;brand;[wy];constructor(n,e){this.reducer=n,this.create=e}};function ao(t){return typeof t=="function"&&t[xy]===!0}var nl=new y("");var il=(()=>{class t{isErrorState(e,i){return!!(e&&e.invalid&&(e.touched||i&&i.submitted))}isSignalErrorState(e){if(!e)return!1;let i=e().invalid(),a=e().touched();return i&&a}static \u0275fac=function(i){return new(i||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})}return t})();var al=class{_defaultMatcher;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;ngControl;formField;constructor(n,e,i,a,r){this._defaultMatcher=n,this._parentFormGroup=i,this._parentForm=a,this._stateChanges=r,e?Lt(e.field)&&!e.updateValueAndValidity?(this.formField=e,this.ngControl=null):(this.formField=null,this.ngControl=e):this.ngControl=this.formField=null}updateErrorState(){let n=this.errorState,e=this._getCurrentErrorState(this.matcher||this._defaultMatcher);e!==n&&(this.errorState=e,this._stateChanges.next())}_getCurrentErrorState(n){if(this.formField&&n?.isSignalErrorState)return n.isSignalErrorState(this.formField.field())??!1;let e=this._parentFormGroup||this._parentForm,i=this.ngControl?this.ngControl.control:null;return n?.isErrorState(i,e)??!1}};var Dp=class{_box;_destroyed=new E;_resizeSubject=new E;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new se(e=>{let i=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),i.unsubscribe(),this._elementObservables.delete(n)}}).pipe(Qe(e=>e.filter(i=>i.target===n)),ot(e=>e.length>0),Xo({bufferSize:1,refCount:!0}),Oe(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},rl=(()=>{class t{_cleanupErrorListener;_observers=new Map;_ngZone=m(N);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,i){let a=i?.box||"content-box";return this._observers.has(a)||this._observers.set(a,new Dp(a)),this._observers.get(a).observe(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})}return t})();var ro=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["mat-label"]]})}return t})(),QI=new y("MatError");var Ip=(()=>{class t{align="start";id=m(Pe).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,a){i&2&&(Gt("id",a.id),K("align",null),S("mat-mdc-form-field-hint-end",a.align==="end"))},inputs:{align:"align",id:"id"}})}return t})(),XI=new y("MatPrefix");var JI=new y("MatSuffix");var Ny=new y("FloatingLabelParent"),ky=(()=>{class t{_elementRef=m(L);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=m(rl);_ngZone=m(N);_parent=m(Ny);_resizeSubscription=new pe;ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return eN(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,a){i&2&&S("mdc-floating-label--float-above",a.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return t})();function eN(t){let n=t;if(n.offsetParent!==null)return n.scrollWidth;let e=n.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let i=e.scrollWidth;return e.remove(),i}var Cy="mdc-line-ripple--active",ol="mdc-line-ripple--deactivating",Sy=(()=>{class t{_elementRef=m(L);_cleanupTransitionEnd;constructor(){let e=m(N),i=m(Se);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(ol),e.add(Cy)}deactivate(){this._elementRef.nativeElement.classList.add(ol)}_handleTransitionEnd=e=>{let i=this._elementRef.nativeElement.classList,a=i.contains(ol);e.propertyName==="opacity"&&a&&i.remove(Cy,ol)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return t})(),Ey=(()=>{class t{_elementRef=m(L);_ngZone=m(N);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,i=e.querySelector(".mdc-floating-label");i?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let i=this._notch.nativeElement;!this.open||!e?i.style.width="":i.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){let e=["notch"];return B({type:t,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(r,o){if(r&1&&Be(e,5),r&2){let s;M(s=T())&&(o._notch=s.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(r,o){r&2&&S("mdc-notched-outline--notched",o.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},ngContentSelectors:["*"],decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(r,o){r&1&&(he(),it(0,"div",1),We(1,"div",2,0),V(3),qe(),it(4,"div",3))},encapsulation:2})})()}return t})(),Np=(()=>{class t{id;ngField=null;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;stateChanges=null;value;static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t})}return t})();var Mp=new y("MatFormField"),tN=new y("MAT_FORM_FIELD_DEFAULT_OPTIONS"),Dy="fill",nN="auto",Iy="fixed",iN="translateY(-50%)",sl=(()=>{class t{_elementRef=m(L);_changeDetectorRef=m(Ie);_platform=m(ye);_idGenerator=m(Pe);_ngZone=m(N);_defaults=m(tN,{optional:!0});_currentDirection;_unwrapMaybeSignal(e){return Lt(e)?e():e}_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=zr("iconPrefixContainer");_textPrefixContainerSignal=zr("textPrefixContainer");_iconSuffixContainerSignal=zr("iconSuffixContainer");_textSuffixContainerSignal=zr("textSuffixContainer");_prefixSuffixContainers=at(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=$v(ro);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=kt(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||nN}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let i=e||this._defaults?.appearance||Dy;this._appearanceSignal.set(i)}_appearanceSignal=ae(Dy);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||Iy}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||Iy}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new E;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=Re();constructor(){let e=this._defaults,i=m(Mt);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),Jt(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=ao(this._control.ngField)?null:this._control.ngControl.control.validator),this._previousControl=this._control,this._changeDetectorRef.markForCheck()),this._control.ngControl&&this._control.ngControl.control&&!ao(this._control.ngField)&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=at(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let i=this._control,a="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(a+e.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(a+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges?.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges?.pipe(yt([void 0,void 0]),Qe(()=>[this._unwrapMaybeSignal(i.errorState),i.userAriaDescribedBy]),Qo(),ot(([[r,o],[s,c]])=>r!==s||o!==c)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&!ao(i.ngField)&&(this._valueChanges=i.ngControl.valueChanges.pipe(Oe(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),jt(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._unwrapMaybeSignal(this._control.focused);e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){Bu({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=at(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._shouldAlwaysFloat()||this._unwrapMaybeSignal(this._control.shouldLabelFloat):!1}_shouldForward(e){let i=this._control?.ngField||this._control?.ngControl;if(!i)return!1;if(ao(i)){let a=i();return e==="valid"?a.valid():e==="dirty"?a.dirty():e==="touched"?a.touched():e==="pending"?a.pending():e==="untouched"?!a.touched():e==="pristine"?!a.dirty():e==="invalid"?!a.valid():!1}else{let a=i;return e==="valid"?a.valid:e==="dirty"?a.dirty:e==="touched"?a.touched:e==="pending"?a.pending:e==="untouched"?a.untouched:e==="pristine"?a.pristine:e==="invalid"?a.invalid:!1}}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._unwrapMaybeSignal(this._control.errorState)?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let r=this._hintChildren?this._hintChildren.find(s=>s.align==="start"):null,o=this._hintChildren?this._hintChildren.find(s=>s.align==="end"):null;r?e.push(r.id):this._hintLabel&&e.push(this._hintLabelId),o&&e.push(o.id)}else this._errorChildren&&e.push(...this._errorChildren.map(r=>r.id));let i=this._control.describedByIds,a;if(i){let r=this._describedByIds||e;a=e.concat(i.filter(o=>o&&!r.includes(o)))}else a=e;this._control.setDescribedByIds(a),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,a=this._iconSuffixContainer?.nativeElement,r=this._textSuffixContainer?.nativeElement,o=e?.getBoundingClientRect().width??0,s=i?.getBoundingClientRect().width??0,c=a?.getBoundingClientRect().width??0,l=r?.getBoundingClientRect().width??0,d=this._currentDirection==="rtl"?"-1":"1",u=`${o+s}px`,f=`calc(${d} * (${u} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,_=`var(--mat-mdc-form-field-label-transform, ${iN} translateX(${f}))`,w=o+s+c+l;return[_,w]}_writeOutlinedLabelStyles(e){if(e!==null){let[i,a]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),a!==null&&this._notchedOutline?._setMaxWidth(a)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let i=e.getRootNode();return i&&i!==e}return document.documentElement.contains(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){let e=["iconPrefixContainer"],i=["textPrefixContainer"],a=["iconSuffixContainer"],r=["textSuffixContainer"],o=["textField"],s=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],c=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function l(re,Ne){re&1&&$(0,"span",21)}function d(re,Ne){if(re&1&&(p(0,"label",20),V(1,1),te(2,l,1,0,"span",21),h()),re&2){let k=x(2);F("floating",k._shouldLabelFloat())("monitorResize",k._hasOutline())("id",k._labelId),K("for",k._control.disableAutomaticLabeling?null:k._control.id),v(2),ne(!k.hideRequiredMarker&&k._unwrapMaybeSignal(k._control.required)?2:-1)}}function u(re,Ne){if(re&1&&te(0,d,3,5,"label",20),re&2){let k=x();ne(k._hasFloatingLabel()?0:-1)}}function g(re,Ne){re&1&&$(0,"div",7)}function f(re,Ne){}function _(re,Ne){if(re&1&&It(0,f,0,0,"ng-template",13),re&2){x(2);let k=et(1);F("ngTemplateOutlet",k)}}function w(re,Ne){if(re&1&&(p(0,"div",9),te(1,_,1,1,null,13),h()),re&2){let k=x();F("matFormFieldNotchedOutlineOpen",k._shouldLabelFloat()),v(),ne(k._forceDisplayInfixLabel()?-1:1)}}function A(re,Ne){re&1&&(p(0,"div",10,2),V(2,2),h())}function ie(re,Ne){re&1&&(p(0,"div",11,3),V(2,3),h())}function fe(re,Ne){}function cn(re,Ne){if(re&1&&It(0,fe,0,0,"ng-template",13),re&2){x();let k=et(1);F("ngTemplateOutlet",k)}}function Wx(re,Ne){re&1&&(p(0,"div",14,4),V(2,4),h())}function qx(re,Ne){re&1&&(p(0,"div",15,5),V(2,5),h())}function Yx(re,Ne){re&1&&$(0,"div",16)}function Zx(re,Ne){re&1&&(p(0,"div",18),V(1,6),h())}function Kx(re,Ne){if(re&1&&(p(0,"mat-hint",22),b(1),h()),re&2){let k=x(2);F("id",k._hintLabelId),v(),de(k.hintLabel)}}function Qx(re,Ne){if(re&1&&(p(0,"div",19),te(1,Kx,2,2,"mat-hint",22),V(2,7),$(3,"div",23),V(4,8),h()),re&2){let k=x();v(),ne(k.hintLabel?1:-1)}}return B({type:t,selectors:[["mat-form-field"]],contentQueries:function(Ne,k,Me){if(Ne&1&&(dc(Me,k._labelChild,ro,5),Ye(Me,Np,5)(Me,XI,5)(Me,JI,5)(Me,QI,5)(Me,Ip,5)),Ne&2){uc();let _t;M(_t=T())&&(k._formFieldControl=_t.first),M(_t=T())&&(k._prefixChildren=_t),M(_t=T())&&(k._suffixChildren=_t),M(_t=T())&&(k._errorChildren=_t),M(_t=T())&&(k._hintChildren=_t)}},viewQuery:function(Ne,k){if(Ne&1&&(mc(k._iconPrefixContainerSignal,e,5)(k._textPrefixContainerSignal,i,5)(k._iconSuffixContainerSignal,a,5)(k._textSuffixContainerSignal,r,5),Be(o,5)(e,5)(i,5)(a,5)(r,5)(ky,5)(Ey,5)(Sy,5)),Ne&2){uc(4);let Me;M(Me=T())&&(k._textField=Me.first),M(Me=T())&&(k._iconPrefixContainer=Me.first),M(Me=T())&&(k._textPrefixContainer=Me.first),M(Me=T())&&(k._iconSuffixContainer=Me.first),M(Me=T())&&(k._textSuffixContainer=Me.first),M(Me=T())&&(k._floatingLabel=Me.first),M(Me=T())&&(k._notchedOutline=Me.first),M(Me=T())&&(k._lineRipple=Me.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(Ne,k){Ne&2&&S("mat-mdc-form-field-label-always-float",k._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",k._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",k._hasIconSuffix)("mat-form-field-invalid",k._unwrapMaybeSignal(k._control.errorState))("mat-form-field-disabled",k._unwrapMaybeSignal(k._control.disabled))("mat-form-field-autofilled",k._unwrapMaybeSignal(k._control.autofilled))("mat-form-field-appearance-fill",k.appearance=="fill")("mat-form-field-appearance-outline",k.appearance=="outline")("mat-form-field-hide-placeholder",k._hasFloatingLabel()&&!k._shouldLabelFloat())("mat-primary",k.color!=="accent"&&k.color!=="warn")("mat-accent",k.color==="accent")("mat-warn",k.color==="warn")("ng-untouched",k._shouldForward("untouched"))("ng-touched",k._shouldForward("touched"))("ng-pristine",k._shouldForward("pristine"))("ng-dirty",k._shouldForward("dirty"))("ng-valid",k._shouldForward("valid"))("ng-invalid",k._shouldForward("invalid"))("ng-pending",k._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[_e([{provide:Mp,useExisting:t},{provide:Ny,useExisting:t}])],ngContentSelectors:c,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(Ne,k){if(Ne&1&&(he(s),It(0,u,1,1,"ng-template",null,0,ci),p(2,"div",6,1),C("click",function(_t){return k._control.onContainerClick(_t)}),te(4,g,1,0,"div",7),p(5,"div",8),te(6,w,2,2,"div",9),te(7,A,3,0,"div",10),te(8,ie,3,0,"div",11),p(9,"div",12),te(10,cn,1,1,null,13),V(11),h(),te(12,Wx,3,0,"div",14),te(13,qx,3,0,"div",15),h(),te(14,Yx,1,0,"div",16),h(),p(15,"div",17),te(16,Zx,2,0,"div",18)(17,Qx,5,1,"div",19),h()),Ne&2){let Me,_t=k._unwrapMaybeSignal(k._control.disabled);v(2),S("mdc-text-field--filled",!k._hasOutline())("mdc-text-field--outlined",k._hasOutline())("mdc-text-field--no-label",!k._hasFloatingLabel())("mdc-text-field--disabled",_t)("mdc-text-field--invalid",k._unwrapMaybeSignal(k._control.errorState)),v(2),ne(!k._hasOutline()&&!_t?4:-1),v(2),ne(k._hasOutline()?6:-1),v(),ne(k._hasIconPrefix?7:-1),v(),ne(k._hasTextPrefix?8:-1),v(2),ne(!k._hasOutline()||k._forceDisplayInfixLabel()?10:-1),v(2),ne(k._hasTextSuffix?12:-1),v(),ne(k._hasIconSuffix?13:-1),v(),ne(k._hasOutline()?-1:14),v(),S("mat-mdc-form-field-subscript-dynamic-size",k.subscriptSizing==="dynamic");let Xx=k._getSubscriptMessageType();v(),ne((Me=Xx)==="error"?16:Me==="hint"?17:-1)}},dependencies:[ky,Ey,Ur,Sy,Ip],styles:[`.mdc-text-field {
  display: inline-flex;
  align-items: baseline;
  padding: 0 16px;
  position: relative;
  box-sizing: border-box;
  overflow: hidden;
  will-change: opacity, transform, color;
  border-top-left-radius: 4px;
  border-top-right-radius: 4px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
}

.mdc-text-field__input {
  width: 100%;
  min-width: 0;
  border: none;
  border-radius: 0;
  background: none;
  padding: 0;
  -moz-appearance: none;
  -webkit-appearance: none;
  height: 28px;
}
.mdc-text-field__input::-webkit-calendar-picker-indicator, .mdc-text-field__input::-webkit-search-cancel-button {
  display: none;
}
.mdc-text-field__input::-ms-clear {
  display: none;
}
.mdc-text-field__input:focus {
  outline: none;
}
.mdc-text-field__input:invalid {
  box-shadow: none;
}
.mdc-text-field__input::placeholder {
  opacity: 0;
}
.mdc-text-field__input::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field__input::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field__input:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mdc-text-field--focused .mdc-text-field__input::placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  opacity: 1;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::placeholder {
  opacity: 0;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--outlined .mdc-text-field__input, .mdc-text-field--filled.mdc-text-field--no-label .mdc-text-field__input {
  height: 100%;
}
.mdc-text-field--outlined .mdc-text-field__input {
  display: flex;
  border: none !important;
  background-color: transparent;
}
.mdc-text-field--disabled .mdc-text-field__input {
  pointer-events: auto;
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--%NS%mat-form-field-filled-input-text-color, var(--%NS%mat-sys-on-surface));
  caret-color: var(--%NS%mat-form-field-filled-caret-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--%NS%mat-form-field-outlined-input-text-color, var(--%NS%mat-sys-on-surface));
  caret-color: var(--%NS%mat-form-field-outlined-caret-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--%NS%mat-form-field-filled-error-caret-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--outlined.mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--%NS%mat-form-field-outlined-error-caret-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--%NS%mat-form-field-filled-disabled-input-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--%NS%mat-form-field-outlined-disabled-input-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-text-field__input {
    background-color: Window;
  }
}

.mdc-text-field--filled {
  height: 56px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
  border-top-left-radius: var(--%NS%mat-form-field-filled-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-top-right-radius: var(--%NS%mat-form-field-filled-container-shape, var(--%NS%mat-sys-corner-extra-small));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) {
  background-color: var(--%NS%mat-form-field-filled-container-color, var(--%NS%mat-sys-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled {
  background-color: var(--%NS%mat-form-field-filled-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 4%, transparent));
}

.mdc-text-field--outlined {
  height: 56px;
  overflow: visible;
  padding-right: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)));
  padding-left: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)) + 4px);
}
[dir=rtl] .mdc-text-field--outlined {
  padding-right: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)) + 4px);
  padding-left: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)));
}

.mdc-floating-label {
  position: absolute;
  left: 0;
  transform-origin: left top;
  line-height: 1.15rem;
  text-align: left;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: text;
  overflow: hidden;
  will-change: transform;
}
[dir=rtl] .mdc-floating-label {
  right: 0;
  left: auto;
  transform-origin: right top;
  text-align: right;
}
.mdc-text-field .mdc-floating-label {
  top: 50%;
  transform: translateY(-50%);
  pointer-events: none;
}
.mdc-notched-outline .mdc-floating-label {
  display: inline-block;
  position: relative;
  max-width: 100%;
}
.mdc-text-field--outlined .mdc-floating-label {
  left: 4px;
  right: auto;
}
[dir=rtl] .mdc-text-field--outlined .mdc-floating-label {
  left: auto;
  right: 4px;
}
.mdc-text-field--filled .mdc-floating-label {
  left: 16px;
  right: auto;
}
[dir=rtl] .mdc-text-field--filled .mdc-floating-label {
  left: auto;
  right: 16px;
}
.mdc-text-field--disabled .mdc-floating-label {
  cursor: default;
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-floating-label {
    z-index: 1;
  }
}
.mdc-text-field--filled.mdc-text-field--no-label .mdc-floating-label {
  display: none;
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-label-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-focus-label-text-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-hover-label-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-error-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-error-focus-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-error-hover-label-text-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-text-field--filled .mdc-floating-label {
  font-family: var(--%NS%mat-form-field-filled-label-text-font, var(--%NS%mat-sys-body-large-font));
  font-size: var(--%NS%mat-form-field-filled-label-text-size, var(--%NS%mat-sys-body-large-size));
  font-weight: var(--%NS%mat-form-field-filled-label-text-weight, var(--%NS%mat-sys-body-large-weight));
  letter-spacing: var(--%NS%mat-form-field-filled-label-text-tracking, var(--%NS%mat-sys-body-large-tracking));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-label-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-focus-label-text-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-hover-label-text-color, var(--%NS%mat-sys-on-surface));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-error-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-error-focus-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-error-hover-label-text-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-text-field--outlined .mdc-floating-label {
  font-family: var(--%NS%mat-form-field-outlined-label-text-font, var(--%NS%mat-sys-body-large-font));
  font-size: var(--%NS%mat-form-field-outlined-label-text-size, var(--%NS%mat-sys-body-large-size));
  font-weight: var(--%NS%mat-form-field-outlined-label-text-weight, var(--%NS%mat-sys-body-large-weight));
  letter-spacing: var(--%NS%mat-form-field-outlined-label-text-tracking, var(--%NS%mat-sys-body-large-tracking));
}

.mdc-floating-label--float-above {
  cursor: auto;
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--filled .mdc-floating-label--float-above {
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--outlined .mdc-floating-label--float-above {
  transform: translateY(-37.25px) scale(1);
  font-size: 0.75rem;
}
.mdc-notched-outline .mdc-floating-label--float-above {
  text-overflow: clip;
}
.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: 133.3333333333%;
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  transform: translateY(-34.75px) scale(0.75);
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: 1rem;
}

.mdc-floating-label--%NS%required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 1px;
  margin-right: 0;
  content: "*";
}
[dir=rtl] .mdc-floating-label--%NS%required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 0;
  margin-right: 1px;
}

.mdc-notched-outline {
  display: flex;
  position: absolute;
  top: 0;
  right: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  max-width: 100%;
  height: 100%;
  text-align: left;
  pointer-events: none;
}
[dir=rtl] .mdc-notched-outline {
  text-align: right;
}
.mdc-text-field--outlined .mdc-notched-outline {
  z-index: 1;
}

.mat-mdc-notch-piece {
  box-sizing: border-box;
  height: 100%;
  pointer-events: none;
  border: none;
  border-top: 1px solid;
  border-bottom: 1px solid;
}
.mdc-text-field--focused .mat-mdc-notch-piece {
  border-width: 2px;
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-outline-color, var(--%NS%mat-sys-outline));
  border-width: var(--%NS%mat-form-field-outlined-outline-width, 1px);
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-hover-outline-color, var(--%NS%mat-sys-on-surface));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-focus-outline-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-disabled-outline-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-error-outline-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--focused):hover .mdc-notched-outline .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-error-hover-outline-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-error-focus-outline-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-notched-outline .mat-mdc-notch-piece {
  border-width: var(--%NS%mat-form-field-outlined-focus-outline-width, 2px);
}

.mdc-notched-outline__leading {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__leading {
  width: max(12px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)));
}
[dir=rtl] .mdc-notched-outline__leading {
  border-left: none;
  border-right: 1px solid;
  border-bottom-left-radius: 0;
  border-top-left-radius: 0;
  border-top-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}

.mdc-notched-outline__trailing {
  flex-grow: 1;
  border-left: none;
  border-right: 1px solid;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  border-top-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}
[dir=rtl] .mdc-notched-outline__trailing {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}

.mdc-notched-outline__notch {
  flex: 0 0 auto;
  width: auto;
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__notch {
  max-width: min(var(--%NS%mat-form-field-notch-max-width, 100%), calc(100% - max(12px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  max-width: min(100%, calc(100% - max(12px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 1px;
}
.mdc-text-field--focused.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 2px;
}
.mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 0;
  padding-right: 8px;
  border-top: none;
}
[dir=rtl] .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 8px;
  padding-right: 0;
}
.mdc-notched-outline--no-label .mdc-notched-outline__notch {
  display: none;
}

.mdc-line-ripple::before, .mdc-line-ripple::after {
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  border-bottom-style: solid;
  content: "";
}
.mdc-line-ripple::before {
  z-index: 1;
  border-bottom-width: var(--%NS%mat-form-field-filled-active-indicator-height, 1px);
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-active-indicator-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-hover-active-indicator-color, var(--%NS%mat-sys-on-surface));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-disabled-active-indicator-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-error-active-indicator-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-error-hover-active-indicator-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-line-ripple::after {
  transform: scaleX(0);
  opacity: 0;
  z-index: 2;
}
.mdc-text-field--filled .mdc-line-ripple::after {
  border-bottom-width: var(--%NS%mat-form-field-filled-focus-active-indicator-height, 2px);
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--%NS%mat-form-field-filled-focus-active-indicator-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--filled.mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--%NS%mat-form-field-filled-error-focus-active-indicator-color, var(--%NS%mat-sys-error));
}

.mdc-line-ripple--%NS%active::after {
  transform: scaleX(1);
  opacity: 1;
}

.mdc-line-ripple--%NS%deactivating::after {
  opacity: 0;
}

.mdc-text-field--disabled {
  pointer-events: none;
}

.mat-mdc-form-field-textarea-control {
  vertical-align: middle;
  resize: vertical;
  box-sizing: border-box;
  height: auto;
  margin: 0;
  padding: 0;
  border: none;
  overflow: auto;
}

.mat-mdc-form-field-input-control.mat-mdc-form-field-input-control {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font: inherit;
  letter-spacing: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  border: none;
}

.mat-mdc-form-field .mat-mdc-floating-label.mdc-floating-label {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  line-height: normal;
  pointer-events: all;
  will-change: auto;
}

.mat-mdc-form-field:not(.mat-form-field-disabled) .mat-mdc-floating-label.mdc-floating-label {
  cursor: inherit;
}

.mdc-text-field--%NS%no-label:not(.mdc-text-field--textarea) .mat-mdc-form-field-input-control.mdc-text-field__input,
.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control {
  height: auto;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control.mdc-text-field__input[type=color] {
  height: 23px;
}

.mat-mdc-text-field-wrapper {
  height: auto;
  flex: auto;
  will-change: auto;
}

.mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-left: 0;
  --%NS%mat-mdc-form-field-label-offset-x: -16px;
}

.mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

[dir=rtl] .mat-mdc-text-field-wrapper {
  padding-left: 16px;
  padding-right: 16px;
}
[dir=rtl] .mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-left: 0;
}
[dir=rtl] .mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

.mat-form-field-disabled .mdc-text-field__input::placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-moz-placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input:-ms-input-placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-label-always-float .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
  opacity: 1;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-infix .mat-mdc-floating-label {
  left: auto;
  right: auto;
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-text-field__input {
  display: inline-block;
}

.mat-mdc-form-field .mat-mdc-text-field-wrapper.mdc-text-field .mdc-notched-outline__notch {
  padding-top: 0;
}

.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: 1px solid transparent;
}

[dir=rtl] .mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: none;
  border-right: 1px solid transparent;
}

.mat-mdc-form-field-infix {
  min-height: var(--%NS%mat-form-field-container-height, 56px);
  padding-top: var(--%NS%mat-form-field-filled-with-label-container-padding-top, 24px);
  padding-bottom: var(--%NS%mat-form-field-filled-with-label-container-padding-bottom, 8px);
}
.mdc-text-field--outlined .mat-mdc-form-field-infix, .mdc-text-field--no-label .mat-mdc-form-field-infix {
  padding-top: var(--%NS%mat-form-field-container-vertical-padding, 16px);
  padding-bottom: var(--%NS%mat-form-field-container-vertical-padding, 16px);
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-flex .mat-mdc-floating-label {
  top: calc(var(--%NS%mat-form-field-container-height, 56px) / 2);
}

.mdc-text-field--filled .mat-mdc-floating-label {
  display: var(--%NS%mat-form-field-filled-label-display, block);
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  --%NS%mat-mdc-form-field-label-transform: translateY(calc(calc(6.75px + var(--%NS%mat-form-field-container-height, 56px) / 2) * -1))
    scale(var(--%NS%mat-mdc-form-field-floating-label-scale, 0.75));
  transform: var(--%NS%mat-mdc-form-field-label-transform);
}

@keyframes _mat-form-field-subscript-animation {
  from {
    opacity: 0;
    transform: translateY(-5px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}
.mat-mdc-form-field-subscript-wrapper {
  box-sizing: border-box;
  width: 100%;
  position: relative;
}

.mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-error-wrapper {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  padding: 0 16px;
  opacity: 1;
  transform: translateY(0);
  animation: _mat-form-field-subscript-animation 0ms cubic-bezier(0.55, 0, 0.55, 0.2);
}

.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-error-wrapper {
  position: static;
}

.mat-mdc-form-field-bottom-align::before {
  content: "";
  display: inline-block;
  height: 16px;
}

.mat-mdc-form-field-bottom-align.mat-mdc-form-field-subscript-dynamic-size::before {
  content: unset;
}

.mat-mdc-form-field-hint-end {
  order: 1;
}

.mat-mdc-form-field-hint-wrapper {
  display: flex;
}

.mat-mdc-form-field-hint-spacer {
  flex: 1 0 1em;
}

.mat-mdc-form-field-error {
  display: block;
  color: var(--%NS%mat-form-field-error-text-color, var(--%NS%mat-sys-error));
}

.mat-mdc-form-field-subscript-wrapper,
.mat-mdc-form-field-bottom-align::before {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--%NS%mat-form-field-subscript-text-font, var(--%NS%mat-sys-body-small-font));
  line-height: var(--%NS%mat-form-field-subscript-text-line-height, var(--%NS%mat-sys-body-small-line-height));
  font-size: var(--%NS%mat-form-field-subscript-text-size, var(--%NS%mat-sys-body-small-size));
  letter-spacing: var(--%NS%mat-form-field-subscript-text-tracking, var(--%NS%mat-sys-body-small-tracking));
  font-weight: var(--%NS%mat-form-field-subscript-text-weight, var(--%NS%mat-sys-body-small-weight));
}

.mat-mdc-form-field-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  opacity: 0;
  pointer-events: none;
  background-color: var(--%NS%mat-form-field-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-focus-overlay {
  opacity: var(--%NS%mat-form-field-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-form-field.mat-focused .mat-mdc-form-field-focus-overlay {
  opacity: var(--%NS%mat-form-field-focus-state-layer-opacity, 0);
}

select.mat-mdc-form-field-input-control {
  -moz-appearance: none;
  -webkit-appearance: none;
  background-color: transparent;
  display: inline-flex;
  box-sizing: border-box;
}
select.mat-mdc-form-field-input-control:not(:disabled) {
  cursor: pointer;
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option {
  color: var(--%NS%mat-form-field-select-option-text-color, var(--%NS%mat-sys-neutral10));
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option:disabled {
  color: var(--%NS%mat-form-field-select-disabled-option-text-color, color-mix(in srgb, var(--%NS%mat-sys-neutral10) 38%, transparent));
}

.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  content: "";
  width: 0;
  height: 0;
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  border-top: 5px solid;
  position: absolute;
  right: 0;
  top: 50%;
  margin-top: -2.5px;
  pointer-events: none;
  color: var(--%NS%mat-form-field-enabled-select-arrow-color, var(--%NS%mat-sys-on-surface-variant));
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  right: auto;
  left: 0;
}
.mat-mdc-form-field-type-mat-native-select.mat-focused .mat-mdc-form-field-infix::after {
  color: var(--%NS%mat-form-field-focus-select-arrow-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-form-field-type-mat-native-select.mat-form-field-disabled .mat-mdc-form-field-infix::after {
  color: var(--%NS%mat-form-field-disabled-select-arrow-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 15px;
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 0;
  padding-left: 15px;
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill .mat-mdc-text-field-wrapper {
    outline: solid 1px;
  }
}
@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-form-field-disabled .mat-mdc-text-field-wrapper {
    outline-color: GrayText;
  }
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-focused .mat-mdc-text-field-wrapper {
    outline: dashed 3px;
  }
}

@media (forced-colors: active) {
  .mat-mdc-form-field.mat-focused .mdc-notched-outline {
    border: dashed 3px;
  }
}

.mat-mdc-form-field-input-control[type=date], .mat-mdc-form-field-input-control[type=datetime], .mat-mdc-form-field-input-control[type=datetime-local], .mat-mdc-form-field-input-control[type=month], .mat-mdc-form-field-input-control[type=week], .mat-mdc-form-field-input-control[type=time] {
  line-height: 1;
}
.mat-mdc-form-field-input-control::-webkit-datetime-edit {
  line-height: 1;
  padding: 0;
  margin-bottom: -2px;
}

.mat-mdc-form-field {
  --%NS%mat-mdc-form-field-floating-label-scale: 0.75;
  display: inline-flex;
  flex-direction: column;
  min-width: 0;
  text-align: left;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--%NS%mat-form-field-container-text-font, var(--%NS%mat-sys-body-large-font));
  line-height: var(--%NS%mat-form-field-container-text-line-height, var(--%NS%mat-sys-body-large-line-height));
  font-size: var(--%NS%mat-form-field-container-text-size, var(--%NS%mat-sys-body-large-size));
  letter-spacing: var(--%NS%mat-form-field-container-text-tracking, var(--%NS%mat-sys-body-large-tracking));
  font-weight: var(--%NS%mat-form-field-container-text-weight, var(--%NS%mat-sys-body-large-weight));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-floating-label--float-above {
  font-size: calc(var(--%NS%mat-form-field-outlined-label-text-populated-size) * var(--%NS%mat-mdc-form-field-floating-label-scale));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: var(--%NS%mat-form-field-outlined-label-text-populated-size);
}
[dir=rtl] .mat-mdc-form-field {
  text-align: right;
}

.mat-mdc-form-field-flex {
  display: inline-flex;
  align-items: baseline;
  box-sizing: border-box;
  width: 100%;
}

.mat-mdc-text-field-wrapper {
  width: 100%;
  z-index: 0;
}

.mat-mdc-form-field-icon-prefix,
.mat-mdc-form-field-icon-suffix {
  align-self: center;
  line-height: 0;
  pointer-events: auto;
  position: relative;
  z-index: 1;
}
.mat-mdc-form-field-icon-prefix > .mat-icon,
.mat-mdc-form-field-icon-suffix > .mat-icon {
  padding: 0 12px;
  box-sizing: content-box;
}

.mat-mdc-form-field-icon-prefix {
  color: var(--%NS%mat-form-field-leading-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-prefix {
  color: var(--%NS%mat-form-field-disabled-leading-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-trailing-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-disabled-trailing-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-invalid .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-error-trailing-icon-color, var(--%NS%mat-sys-error));
}
.mat-form-field-invalid:not(.mat-focused):not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-error-hover-trailing-icon-color, var(--%NS%mat-sys-on-error-container));
}
.mat-form-field-invalid.mat-focused .mat-mdc-text-field-wrapper .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-error-focus-trailing-icon-color, var(--%NS%mat-sys-error));
}

.mat-mdc-form-field-icon-prefix,
[dir=rtl] .mat-mdc-form-field-icon-suffix {
  padding: 0 4px 0 0;
}

.mat-mdc-form-field-icon-suffix,
[dir=rtl] .mat-mdc-form-field-icon-prefix {
  padding: 0 0 0 4px;
}

.mat-mdc-form-field-subscript-wrapper .mat-icon,
.mat-mdc-form-field label .mat-icon {
  width: 1em;
  height: 1em;
  font-size: inherit;
}

.mat-mdc-form-field-infix {
  flex: auto;
  min-width: 0;
  width: 180px;
  position: relative;
  box-sizing: border-box;
}
.mat-mdc-form-field-infix:has(textarea[cols]) {
  width: auto;
}

.mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: -1px;
  -webkit-clip-path: inset(-9em -999em -9em 1px);
  clip-path: inset(-9em -999em -9em 1px);
}
[dir=rtl] .mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: 0;
  margin-right: -1px;
  -webkit-clip-path: inset(-9em 1px -9em -999em);
  clip-path: inset(-9em 1px -9em -999em);
}

.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-floating-label {
  transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input {
  transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-moz-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-webkit-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input:-ms-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field--%NS%filled:not(.mdc-ripple-upgraded):focus .mdc-text-field__ripple::before {
  transition-duration: 75ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-line-ripple::after {
  transition: transform 180ms cubic-bezier(0.4, 0, 0.2, 1), opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-error-wrapper {
  animation-duration: 300ms;
}

.mdc-notched-outline .mdc-floating-label {
  max-width: calc(100% + 1px);
}

.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: calc(133.3333333333% + 1px);
}
`],encapsulation:2})})()}return t})();var Rp=new y("mat-chips-default-options",{providedIn:"root",factory:()=>({separatorKeyCodes:[13]})}),My=new y("MatChipAvatar"),Ty=new y("MatChipTrailingIcon"),Ay=new y("MatChipEdit"),Ry=new y("MatChipRemove"),Op=new y("MatChip"),Oy=(()=>{class t{_elementRef=m(L);_parentChip=m(Op);_isPrimary=!0;_isLeading=!1;get disabled(){return this._disabled||this._parentChip?.disabled||!1}set disabled(e){this._disabled=e}_disabled=!1;tabIndex=-1;_allowFocusWhenDisabled=!1;_getDisabledAttribute(){return this.disabled&&!this._allowFocusWhenDisabled?"":null}constructor(){m(je).load(Ct),this._elementRef.nativeElement.nodeName==="BUTTON"&&this._elementRef.nativeElement.setAttribute("type","button")}focus(){this._elementRef.nativeElement.focus()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["","matChipContent",""]],hostAttrs:[1,"mat-mdc-chip-action","mdc-evolution-chip__action","mdc-evolution-chip__action--presentational"],hostVars:8,hostBindings:function(i,a){i&2&&(K("disabled",a._getDisabledAttribute())("aria-disabled",a.disabled),S("mdc-evolution-chip__action--primary",a._isPrimary)("mdc-evolution-chip__action--secondary",!a._isPrimary)("mdc-evolution-chip__action--trailing",!a._isPrimary&&!a._isLeading))},inputs:{disabled:[2,"disabled","disabled",I],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?-1:ht(e)],_allowFocusWhenDisabled:"_allowFocusWhenDisabled"}})}return t})(),Fy=(()=>{class t extends Oy{_getTabindex(){return this.disabled&&!this._allowFocusWhenDisabled?null:this.tabIndex.toString()}_handleClick(e){!this.disabled&&this._isPrimary&&(e.preventDefault(),this._parentChip._handlePrimaryActionInteraction())}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!this.disabled&&this._isPrimary&&!this._parentChip._isEditing&&(e.preventDefault(),this._parentChip._handlePrimaryActionInteraction())}static \u0275fac=(()=>{let e;return function(a){return(e||(e=Fe(t)))(a||t)}})();static \u0275dir=D({type:t,selectors:[["","matChipAction",""]],hostVars:3,hostBindings:function(i,a){i&1&&C("click",function(o){return a._handleClick(o)})("keydown",function(o){return a._handleKeydown(o)}),i&2&&(K("tabindex",a._getTabindex()),S("mdc-evolution-chip__action--presentational",!1))},features:[le]})}return t})();var Tp=(()=>{class t{_changeDetectorRef=m(Ie);_elementRef=m(L);_tagName=m(Uv);_ngZone=m(N);_focusMonitor=m(on);_globalRippleOptions=m(mi,{optional:!0});_document=m(ee);_onFocus=new E;_onBlur=new E;_isBasicChip=!1;role=null;_hasFocusInternal=!1;_pendingFocus=!1;_actionChanges;_animationsDisabled=Re();_allLeadingIcons;_allTrailingIcons;_allEditIcons;_allRemoveIcons;_hasFocus(){return this._hasFocusInternal}id=m(Pe).getId("mat-mdc-chip-");ariaLabel=null;ariaDescription=null;_chipListDisabled=!1;_hadFocusOnRemove=!1;_textElement;get value(){return this._value!==void 0?this._value:this._textElement.textContent.trim()}set value(e){this._value=e}_value;color;removable=!0;highlighted=!1;disableRipple=!1;get disabled(){return this._disabled||this._chipListDisabled}set disabled(e){this._disabled=e}_disabled=!1;removed=new z;destroyed=new z;basicChipAttrName="mat-basic-chip";leadingIcon;editIcon;trailingIcon;removeIcon;primaryAction;_rippleLoader=m(Xc);_injector=m(X);constructor(){let e=m(je);e.load(Ct),e.load(fp),this._monitorFocus(),this._rippleLoader?.configureRipple(this._elementRef.nativeElement,{className:"mat-mdc-chip-ripple",disabled:this._isRippleDisabled()})}ngOnInit(){this._isBasicChip=this._elementRef.nativeElement.hasAttribute(this.basicChipAttrName)||this._tagName.toLowerCase()===this.basicChipAttrName}ngAfterViewInit(){this._textElement=this._elementRef.nativeElement.querySelector(".mat-mdc-chip-action-label"),this._pendingFocus&&(this._pendingFocus=!1,this.focus())}ngAfterContentInit(){this._actionChanges=jt(this._allLeadingIcons.changes,this._allTrailingIcons.changes,this._allEditIcons.changes,this._allRemoveIcons.changes).subscribe(()=>this._changeDetectorRef.markForCheck())}ngDoCheck(){this._rippleLoader.setDisabled(this._elementRef.nativeElement,this._isRippleDisabled())}ngOnDestroy(){this.destroyed.emit({chip:this}),this.destroyed.complete(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement),this._actionChanges?.unsubscribe()}remove(){this.removable&&(this._hadFocusOnRemove=this._hasFocus(),this.removed.emit({chip:this}))}_isRippleDisabled(){return this.disabled||this.disableRipple||this._animationsDisabled||this._isBasicChip||!this._hasInteractiveActions()||!!this._globalRippleOptions?.disabled}_hasTrailingIcon(){return!!(this.trailingIcon||this.removeIcon)}_handleKeydown(e){(e.keyCode===8&&!e.repeat||e.keyCode===46)&&(e.preventDefault(),this.remove())}focus(){this.disabled||(this.primaryAction?this.primaryAction.focus():this._pendingFocus=!0)}_getSourceAction(e){return this._getActions().find(i=>{let a=i._elementRef.nativeElement;return a===e||a.contains(e)})}_getActions(){let e=[];return this.editIcon&&e.push(this.editIcon),this.primaryAction&&e.push(this.primaryAction),this.removeIcon&&e.push(this.removeIcon),e}_handlePrimaryActionInteraction(){}_hasInteractiveActions(){return this._getActions().length>0}_edit(e){}_monitorFocus(){this._focusMonitor.monitor(this._elementRef,!0).subscribe(e=>{let i=e!==null;i!==this._hasFocusInternal&&(this._hasFocusInternal=i,i?this._onFocus.next({chip:this}):(this._changeDetectorRef.markForCheck(),setTimeout(()=>this._ngZone.run(()=>this._onBlur.next({chip:this})))))})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){let e=["*",[["mat-chip-avatar"],["","matChipAvatar",""]],[["mat-chip-trailing-icon"],["","matChipRemove",""],["","matChipTrailingIcon",""]]],i=["*","mat-chip-avatar, [matChipAvatar]","mat-chip-trailing-icon,[matChipRemove],[matChipTrailingIcon]"];function a(o,s){o&1&&(p(0,"span",3),V(1,1),h())}function r(o,s){o&1&&(p(0,"span",7),V(1,2),h())}return B({type:t,selectors:[["mat-basic-chip"],["","mat-basic-chip",""],["mat-chip"],["","mat-chip",""]],contentQueries:function(s,c,l){if(s&1&&Ye(l,My,5)(l,Ay,5)(l,Ty,5)(l,Ry,5)(l,My,5)(l,Ty,5)(l,Ay,5)(l,Ry,5),s&2){let d;M(d=T())&&(c.leadingIcon=d.first),M(d=T())&&(c.editIcon=d.first),M(d=T())&&(c.trailingIcon=d.first),M(d=T())&&(c.removeIcon=d.first),M(d=T())&&(c._allLeadingIcons=d),M(d=T())&&(c._allTrailingIcons=d),M(d=T())&&(c._allEditIcons=d),M(d=T())&&(c._allRemoveIcons=d)}},viewQuery:function(s,c){if(s&1&&Be(Fy,5),s&2){let l;M(l=T())&&(c.primaryAction=l.first)}},hostAttrs:[1,"mat-mdc-chip"],hostVars:31,hostBindings:function(s,c){s&1&&C("keydown",function(d){return c._handleKeydown(d)}),s&2&&(Gt("id",c.id),K("role",c.role)("aria-label",c.ariaLabel),xt("mat-"+(c.color||"primary")),S("mdc-evolution-chip",!c._isBasicChip)("mdc-evolution-chip--disabled",c.disabled)("mdc-evolution-chip--with-trailing-action",c._hasTrailingIcon())("mdc-evolution-chip--with-primary-graphic",c.leadingIcon)("mdc-evolution-chip--with-primary-icon",c.leadingIcon)("mdc-evolution-chip--with-avatar",c.leadingIcon)("mat-mdc-chip-with-avatar",c.leadingIcon)("mat-mdc-chip-highlighted",c.highlighted)("mat-mdc-chip-disabled",c.disabled)("mat-mdc-basic-chip",c._isBasicChip)("mat-mdc-standard-chip",!c._isBasicChip)("mat-mdc-chip-with-trailing-icon",c._hasTrailingIcon())("_mat-animation-noopable",c._animationsDisabled))},inputs:{role:"role",id:"id",ariaLabel:[0,"aria-label","ariaLabel"],ariaDescription:[0,"aria-description","ariaDescription"],value:"value",color:"color",removable:[2,"removable","removable",I],highlighted:[2,"highlighted","highlighted",I],disableRipple:[2,"disableRipple","disableRipple",I],disabled:[2,"disabled","disabled",I]},outputs:{removed:"removed",destroyed:"destroyed"},exportAs:["matChip"],features:[_e([{provide:Op,useExisting:t}])],ngContentSelectors:i,decls:9,vars:2,consts:[[1,"mat-mdc-chip-focus-overlay"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--primary"],["matChipContent",""],[1,"mdc-evolution-chip__graphic","mat-mdc-chip-graphic"],[1,"mdc-evolution-chip__text-label","mat-mdc-chip-action-label"],[1,"mat-mdc-chip-primary-focus-indicator","mat-focus-indicator"],[1,"mat-mdc-chip-touch-target"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--trailing"]],template:function(s,c){s&1&&(he(e),$(0,"span",0),p(1,"span",1)(2,"span",2),te(3,a,2,0,"span",3),p(4,"span",4),V(5),$(6,"span",5),h(),$(7,"span",6),h()(),te(8,r,2,0,"span",7)),s&2&&(v(3),ne(c.leadingIcon?3:-1),v(5),ne(c._hasTrailingIcon()?8:-1))},dependencies:[Oy],styles:[`.mdc-evolution-chip,
.mdc-evolution-chip__cell,
.mdc-evolution-chip__action {
  display: inline-flex;
  align-items: center;
}

.mdc-evolution-chip {
  position: relative;
  max-width: 100%;
}

.mat-mdc-chip-touch-target {
  position: absolute;
  top: 50%;
  left: 0;
  right: 0;
  height: 48px;
  transform: translateY(-50%);
  display: var(--%NS%mat-chip-touch-target-display, block);
  pointer-events: auto;
}

.mdc-evolution-chip__cell,
.mdc-evolution-chip__action {
  height: 100%;
}

.mdc-evolution-chip__cell--primary {
  flex-basis: 100%;
  overflow-x: hidden;
}

.mdc-evolution-chip__cell--trailing {
  flex: 1 0 auto;
}

.mdc-evolution-chip__action {
  align-items: center;
  background: none;
  border: none;
  box-sizing: content-box;
  cursor: pointer;
  display: inline-flex;
  justify-content: center;
  outline: none;
  padding: 0;
  text-decoration: none;
  color: inherit;
}

.mdc-evolution-chip__action--presentational {
  cursor: auto;
}

.mdc-evolution-chip--disabled,
.mdc-evolution-chip__action:disabled {
  pointer-events: none;
}
@media (forced-colors: active) {
  .mdc-evolution-chip--disabled,
  .mdc-evolution-chip__action:disabled {
    forced-color-adjust: none;
  }
}

.mdc-evolution-chip__action--primary {
  font: inherit;
  letter-spacing: inherit;
  white-space: inherit;
  overflow-x: hidden;
}
.mat-mdc-standard-chip .mdc-evolution-chip__action--%NS%primary::before {
  border-width: var(--%NS%mat-chip-outline-width, 1px);
  border-radius: var(--%NS%mat-chip-container-shape-radius, 8px);
  box-sizing: border-box;
  content: "";
  height: 100%;
  left: 0;
  position: absolute;
  pointer-events: none;
  top: 0;
  width: 100%;
  z-index: 1;
  border-style: solid;
}
.mat-mdc-standard-chip .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 12px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--%NS%primary::before {
  border-color: var(--%NS%mat-chip-outline-color, var(--%NS%mat-sys-outline));
}
.mdc-evolution-chip__action--%NS%primary:not(.mdc-evolution-chip__action--presentational):not(.mdc-ripple-upgraded):focus::before {
  border-color: var(--%NS%mat-chip-focus-outline-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--%NS%primary::before {
  border-color: var(--%NS%mat-chip-disabled-outline-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__action--%NS%primary::before {
  border-width: var(--%NS%mat-chip-flat-selected-outline-width, 0);
}
.mat-mdc-basic-chip .mdc-evolution-chip__action--primary {
  font: inherit;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}

.mdc-evolution-chip__action--secondary {
  position: relative;
  overflow: visible;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--secondary {
  color: var(--%NS%mat-chip-with-trailing-icon-trailing-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--secondary {
  color: var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, [dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}

.mdc-evolution-chip__text-label {
  -webkit-user-select: none;
  user-select: none;
  white-space: nowrap;
  text-overflow: ellipsis;
  overflow: hidden;
}
.mat-mdc-standard-chip .mdc-evolution-chip__text-label {
  font-family: var(--%NS%mat-chip-label-text-font, var(--%NS%mat-sys-label-large-font));
  line-height: var(--%NS%mat-chip-label-text-line-height, var(--%NS%mat-sys-label-large-line-height));
  font-size: var(--%NS%mat-chip-label-text-size, var(--%NS%mat-sys-label-large-size));
  font-weight: var(--%NS%mat-chip-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  letter-spacing: var(--%NS%mat-chip-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {
  color: var(--%NS%mat-chip-label-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--%NS%selected:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {
  color: var(--%NS%mat-chip-selected-label-text-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label, .mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label {
  color: var(--%NS%mat-chip-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mdc-evolution-chip__graphic {
  align-items: center;
  display: inline-flex;
  justify-content: center;
  overflow: hidden;
  pointer-events: none;
  position: relative;
  flex: 1 0 auto;
}
.mat-mdc-standard-chip .mdc-evolution-chip__graphic {
  width: var(--%NS%mat-chip-with-avatar-avatar-size, 24px);
  height: var(--%NS%mat-chip-with-avatar-avatar-size, 24px);
  font-size: var(--%NS%mat-chip-with-avatar-avatar-size, 24px);
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__graphic {
  transition: width 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mdc-evolution-chip--%NS%selectable:not(.mdc-evolution-chip--selected):not(.mdc-evolution-chip--with-primary-icon) .mdc-evolution-chip__graphic {
  width: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 6px;
  padding-right: 6px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 4px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 8px;
  padding-right: 4px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 6px;
  padding-right: 6px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 4px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 8px;
  padding-right: 4px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__graphic {
  padding-left: 0;
}

.mdc-evolution-chip__checkmark {
  position: absolute;
  opacity: 0;
  top: 50%;
  left: 50%;
  height: 20px;
  width: 20px;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__checkmark {
  color: var(--%NS%mat-chip-with-icon-selected-icon-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__checkmark {
  color: var(--%NS%mat-chip-with-icon-disabled-icon-color, var(--%NS%mat-sys-on-surface));
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark {
  transition: transform 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  transform: translate(-75%, -50%);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {
  transform: translate(-50%, -50%);
  opacity: 1;
}

.mdc-evolution-chip__checkmark-svg {
  display: block;
}

.mdc-evolution-chip__checkmark-path {
  stroke-width: 2px;
  stroke-dasharray: 29.7833385;
  stroke-dashoffset: 29.7833385;
  stroke: currentColor;
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark-path {
  transition: stroke-dashoffset 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark-path {
  stroke-dashoffset: 0;
}
@media (forced-colors: active) {
  .mdc-evolution-chip__checkmark-path {
    stroke: CanvasText !important;
  }
}

.mat-mdc-standard-chip .mdc-evolution-chip__icon--trailing {
  height: 18px;
  width: 18px;
  font-size: 18px;
}
.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove {
  opacity: calc(var(--%NS%mat-chip-trailing-action-opacity, 1) * var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));
}
.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove:focus {
  opacity: calc(var(--%NS%mat-chip-trailing-action-focus-opacity, 1) * var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));
}

.mat-mdc-standard-chip {
  border-radius: var(--%NS%mat-chip-container-shape-radius, 8px);
  height: var(--%NS%mat-chip-container-height, 32px);
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) {
  background-color: var(--%NS%mat-chip-elevated-container-color, transparent);
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled {
  background-color: var(--%NS%mat-chip-elevated-disabled-container-color);
}
.mat-mdc-standard-chip.mdc-evolution-chip--%NS%selected:not(.mdc-evolution-chip--disabled) {
  background-color: var(--%NS%mat-chip-elevated-selected-container-color, var(--%NS%mat-sys-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled {
  background-color: var(--%NS%mat-chip-flat-disabled-selected-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-standard-chip {
    outline: solid 1px;
  }
}

.mat-mdc-standard-chip .mdc-evolution-chip__icon--primary {
  border-radius: var(--%NS%mat-chip-with-avatar-avatar-shape-radius, 24px);
  width: var(--%NS%mat-chip-with-icon-icon-size, 18px);
  height: var(--%NS%mat-chip-with-icon-icon-size, 18px);
  font-size: var(--%NS%mat-chip-with-icon-icon-size, 18px);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__icon--primary {
  opacity: 0;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__icon--primary {
  color: var(--%NS%mat-chip-with-icon-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--primary {
  color: var(--%NS%mat-chip-with-icon-disabled-icon-color, var(--%NS%mat-sys-on-surface));
}

.mat-mdc-chip-highlighted {
  --%NS%mat-chip-with-icon-icon-color: var(--%NS%mat-chip-with-icon-selected-icon-color, var(--%NS%mat-sys-on-secondary-container));
  --%NS%mat-chip-elevated-container-color: var(--%NS%mat-chip-elevated-selected-container-color, var(--%NS%mat-sys-secondary-container));
  --%NS%mat-chip-label-text-color: var(--%NS%mat-chip-selected-label-text-color, var(--%NS%mat-sys-on-secondary-container));
  --%NS%mat-chip-outline-width: var(--%NS%mat-chip-flat-selected-outline-width, 0);
}

.mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-focus-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-chip-selected .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted .mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-selected-focus-state-layer-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-mdc-chip:hover .mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-hover-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
  opacity: var(--%NS%mat-chip-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-chip-focus-overlay .mat-mdc-chip-selected:hover, .mat-mdc-chip-highlighted:hover .mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-selected-hover-state-layer-color, var(--%NS%mat-sys-on-secondary-container));
  opacity: var(--%NS%mat-chip-selected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-chip.cdk-focused .mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-focus-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
  opacity: var(--%NS%mat-chip-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-chip-selected.cdk-focused .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted.cdk-focused .mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-selected-focus-state-layer-color, var(--%NS%mat-sys-on-secondary-container));
  opacity: var(--%NS%mat-chip-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}

.mdc-evolution-chip--%NS%disabled:not(.mdc-evolution-chip--selected) .mat-mdc-chip-avatar {
  opacity: var(--%NS%mat-chip-with-avatar-disabled-avatar-opacity, 0.38);
}

.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {
  opacity: var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38);
}

.mdc-evolution-chip--disabled.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {
  opacity: var(--%NS%mat-chip-with-icon-disabled-icon-opacity, 0.38);
}

.mat-mdc-standard-chip.mdc-evolution-chip--disabled {
  opacity: var(--%NS%mat-chip-disabled-container-opacity, 1);
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted .mdc-evolution-chip__icon--trailing {
  color: var(--%NS%mat-chip-selected-trailing-icon-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {
  color: var(--%NS%mat-chip-selected-disabled-trailing-icon-color, var(--%NS%mat-sys-on-surface));
}

.mat-mdc-chip-edit, .mat-mdc-chip-remove {
  opacity: var(--%NS%mat-chip-trailing-action-opacity, 1);
}
.mat-mdc-chip-edit:focus, .mat-mdc-chip-remove:focus {
  opacity: var(--%NS%mat-chip-trailing-action-focus-opacity, 1);
}
.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {
  background-color: var(--%NS%mat-chip-trailing-action-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-chip-edit:hover::after, .mat-mdc-chip-remove:hover::after {
  opacity: calc(var(--%NS%mat-chip-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)));
}
.mat-mdc-chip-edit:focus::after, .mat-mdc-chip-remove:focus::after {
  opacity: calc(var(--%NS%mat-chip-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)));
}

.mat-mdc-chip-selected .mat-mdc-chip-remove::after,
.mat-mdc-chip-highlighted .mat-mdc-chip-remove::after {
  background-color: var(--%NS%mat-chip-selected-trailing-action-state-layer-color, var(--%NS%mat-sys-on-secondary-container));
}

.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:focus::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:focus::after {
  opacity: calc(var(--%NS%mat-chip-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)));
}
.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:hover::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:hover::after {
  opacity: calc(var(--%NS%mat-chip-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)));
}

.mat-mdc-standard-chip {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-standard-chip .mat-mdc-chip-graphic,
.mat-mdc-standard-chip .mat-mdc-chip-trailing-icon {
  box-sizing: content-box;
}
.mat-mdc-standard-chip._mat-animation-noopable,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__graphic,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark-path {
  transition-duration: 1ms;
  animation-duration: 1ms;
}

.mat-mdc-chip-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  opacity: 0;
  border-radius: inherit;
  transition: opacity 150ms linear;
}
._mat-animation-noopable .mat-mdc-chip-focus-overlay {
  transition: none;
}
.mat-mdc-basic-chip .mat-mdc-chip-focus-overlay {
  display: none;
}

.mat-mdc-chip .mat-ripple.mat-mdc-chip-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}

.mat-mdc-chip-avatar {
  text-align: center;
  line-height: 1;
  color: var(--%NS%mat-chip-with-icon-icon-color, currentColor);
}

.mat-mdc-chip {
  position: relative;
  z-index: 0;
}

.mat-mdc-chip-action-label {
  text-align: left;
  z-index: 1;
}
[dir=rtl] .mat-mdc-chip-action-label {
  text-align: right;
}
.mat-mdc-chip.mdc-evolution-chip--with-trailing-action .mat-mdc-chip-action-label {
  position: relative;
}
.mat-mdc-chip-action-label .mat-mdc-chip-primary-focus-indicator {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  pointer-events: none;
}
.mat-mdc-chip-action-label .mat-focus-indicator::before {
  margin: calc(calc(var(--%NS%mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-chip-edit::before, .mat-mdc-chip-remove::before {
  margin: calc(var(--%NS%mat-focus-indicator-border-width, 3px) * -1);
  left: 8px;
  right: 8px;
}
.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {
  content: "";
  display: block;
  opacity: 0;
  position: absolute;
  top: -3px;
  bottom: -3px;
  left: 5px;
  right: 5px;
  border-radius: 50%;
  box-sizing: border-box;
  padding: 12px;
  margin: -12px;
  background-clip: content-box;
}
.mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {
  width: 18px;
  height: 18px;
  font-size: 18px;
  box-sizing: content-box;
}

.mat-chip-edit-input {
  cursor: text;
  display: inline-block;
  color: inherit;
  outline: 0;
}

@media (forced-colors: active) {
  .mat-mdc-chip-selected:not(.mat-mdc-chip-multiple) {
    outline-width: 3px;
  }
}

.mat-mdc-chip-action:focus-visible .mat-focus-indicator::before {
  content: "";
}

.mdc-evolution-chip__icon, .mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {
  min-height: fit-content;
}

img.mdc-evolution-chip__icon {
  min-height: 0;
}
`],encapsulation:2})})()}return t})();var Fp=(()=>{class t extends Tp{_defaultOptions=m(Rp,{optional:!0});chipListSelectable=!0;_chipListMultiple=!1;_chipListHideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get selectable(){return this._selectable&&this.chipListSelectable}set selectable(e){this._selectable=e,this._changeDetectorRef.markForCheck()}_selectable=!0;get selected(){return this._selected}set selected(e){this._setSelectedState(e,!1,!0)}_selected=!1;get ariaSelected(){return this.selectable?this.selected.toString():null}basicChipAttrName="mat-basic-chip-option";selectionChange=new z;ngOnInit(){super.ngOnInit(),this.role="presentation"}select(){this._setSelectedState(!0,!1,!0)}deselect(){this._setSelectedState(!1,!1,!0)}selectViaInteraction(){this._setSelectedState(!0,!0,!0)}toggleSelected(e=!1){return this._setSelectedState(!this.selected,e,!0),this.selected}_handlePrimaryActionInteraction(){this.disabled||(this.focus(),this.selectable&&this.toggleSelected(!0))}_hasLeadingGraphic(){return this.leadingIcon?!0:!this._chipListHideSingleSelectionIndicator||this._chipListMultiple}_setSelectedState(e,i,a){e!==this.selected&&(this._selected=e,a&&this.selectionChange.emit({source:this,isUserInput:i,selected:this.selected}),this._changeDetectorRef.markForCheck())}static \u0275fac=(()=>{let e;return function(a){return(e||(e=Fe(t)))(a||t)}})();static \u0275cmp=(function(){let e=["*",[["mat-chip-avatar"],["","matChipAvatar",""]],[["mat-chip-trailing-icon"],["","matChipRemove",""],["","matChipTrailingIcon",""]]],i=["*","mat-chip-avatar, [matChipAvatar]","mat-chip-trailing-icon,[matChipRemove],[matChipTrailingIcon]"];function a(o,s){o&1&&(p(0,"span",3),V(1,1),p(2,"span",8),Ge(),p(3,"svg",9),$(4,"path",10),h()()())}function r(o,s){o&1&&(p(0,"span",7),V(1,2),h())}return B({type:t,selectors:[["mat-basic-chip-option"],["","mat-basic-chip-option",""],["mat-chip-option"],["","mat-chip-option",""]],hostAttrs:[1,"mat-mdc-chip","mat-mdc-chip-option"],hostVars:37,hostBindings:function(s,c){s&2&&(Gt("id",c.id),K("tabindex",null)("aria-label",null)("aria-description",null)("role",c.role),S("mdc-evolution-chip",!c._isBasicChip)("mdc-evolution-chip--filter",!c._isBasicChip)("mdc-evolution-chip--selectable",!c._isBasicChip)("mat-mdc-chip-selected",c.selected)("mat-mdc-chip-multiple",c._chipListMultiple)("mat-mdc-chip-disabled",c.disabled)("mat-mdc-chip-with-avatar",c.leadingIcon)("mdc-evolution-chip--disabled",c.disabled)("mdc-evolution-chip--selected",c.selected)("mdc-evolution-chip--selecting",!c._animationsDisabled)("mdc-evolution-chip--with-trailing-action",c._hasTrailingIcon())("mdc-evolution-chip--with-primary-icon",c.leadingIcon)("mdc-evolution-chip--with-primary-graphic",c._hasLeadingGraphic())("mdc-evolution-chip--with-avatar",c.leadingIcon)("mat-mdc-chip-highlighted",c.highlighted)("mat-mdc-chip-with-trailing-icon",c._hasTrailingIcon()))},inputs:{selectable:[2,"selectable","selectable",I],selected:[2,"selected","selected",I]},outputs:{selectionChange:"selectionChange"},features:[_e([{provide:Tp,useExisting:t},{provide:Op,useExisting:t}]),le],ngContentSelectors:i,decls:9,vars:6,consts:[[1,"mat-mdc-chip-focus-overlay"],["role","presentation",1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--primary"],["matChipAction","","role","option",3,"_allowFocusWhenDisabled"],[1,"mdc-evolution-chip__graphic","mat-mdc-chip-graphic"],[1,"mdc-evolution-chip__text-label","mat-mdc-chip-action-label"],[1,"mat-mdc-chip-primary-focus-indicator","mat-focus-indicator"],[1,"mat-mdc-chip-touch-target"],["role","presentation",1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--trailing"],[1,"mdc-evolution-chip__checkmark"],["viewBox","-2 -3 30 30","focusable","false","aria-hidden","true",1,"mdc-evolution-chip__checkmark-svg"],["fill","none","stroke","currentColor","d","M1.73,12.91 8.1,19.28 22.79,4.59",1,"mdc-evolution-chip__checkmark-path"]],template:function(s,c){s&1&&(he(e),$(0,"span",0),p(1,"span",1)(2,"button",2),te(3,a,5,0,"span",3),p(4,"span",4),V(5),$(6,"span",5)(7,"span",6),h()()(),te(8,r,2,0,"span",7)),s&2&&(v(2),F("_allowFocusWhenDisabled",!0),K("aria-description",c.ariaDescription)("aria-label",c.ariaLabel)("aria-selected",c.ariaSelected),v(),ne(c._hasLeadingGraphic()?3:-1),v(5),ne(c._hasTrailingIcon()?8:-1))},dependencies:[Fy],styles:[`.mdc-evolution-chip,
.mdc-evolution-chip__cell,
.mdc-evolution-chip__action {
  display: inline-flex;
  align-items: center;
}

.mdc-evolution-chip {
  position: relative;
  max-width: 100%;
}

.mat-mdc-chip-touch-target {
  position: absolute;
  top: 50%;
  left: 0;
  right: 0;
  height: 48px;
  transform: translateY(-50%);
  display: var(--%NS%mat-chip-touch-target-display, block);
  pointer-events: auto;
}

.mdc-evolution-chip__cell,
.mdc-evolution-chip__action {
  height: 100%;
}

.mdc-evolution-chip__cell--primary {
  flex-basis: 100%;
  overflow-x: hidden;
}

.mdc-evolution-chip__cell--trailing {
  flex: 1 0 auto;
}

.mdc-evolution-chip__action {
  align-items: center;
  background: none;
  border: none;
  box-sizing: content-box;
  cursor: pointer;
  display: inline-flex;
  justify-content: center;
  outline: none;
  padding: 0;
  text-decoration: none;
  color: inherit;
}

.mdc-evolution-chip__action--presentational {
  cursor: auto;
}

.mdc-evolution-chip--disabled,
.mdc-evolution-chip__action:disabled {
  pointer-events: none;
}
@media (forced-colors: active) {
  .mdc-evolution-chip--disabled,
  .mdc-evolution-chip__action:disabled {
    forced-color-adjust: none;
  }
}

.mdc-evolution-chip__action--primary {
  font: inherit;
  letter-spacing: inherit;
  white-space: inherit;
  overflow-x: hidden;
}
.mat-mdc-standard-chip .mdc-evolution-chip__action--%NS%primary::before {
  border-width: var(--%NS%mat-chip-outline-width, 1px);
  border-radius: var(--%NS%mat-chip-container-shape-radius, 8px);
  box-sizing: border-box;
  content: "";
  height: 100%;
  left: 0;
  position: absolute;
  pointer-events: none;
  top: 0;
  width: 100%;
  z-index: 1;
  border-style: solid;
}
.mat-mdc-standard-chip .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 12px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--%NS%primary::before {
  border-color: var(--%NS%mat-chip-outline-color, var(--%NS%mat-sys-outline));
}
.mdc-evolution-chip__action--%NS%primary:not(.mdc-evolution-chip__action--presentational):not(.mdc-ripple-upgraded):focus::before {
  border-color: var(--%NS%mat-chip-focus-outline-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--%NS%primary::before {
  border-color: var(--%NS%mat-chip-disabled-outline-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__action--%NS%primary::before {
  border-width: var(--%NS%mat-chip-flat-selected-outline-width, 0);
}
.mat-mdc-basic-chip .mdc-evolution-chip__action--primary {
  font: inherit;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}

.mdc-evolution-chip__action--secondary {
  position: relative;
  overflow: visible;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--secondary {
  color: var(--%NS%mat-chip-with-trailing-icon-trailing-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--secondary {
  color: var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, [dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}

.mdc-evolution-chip__text-label {
  -webkit-user-select: none;
  user-select: none;
  white-space: nowrap;
  text-overflow: ellipsis;
  overflow: hidden;
}
.mat-mdc-standard-chip .mdc-evolution-chip__text-label {
  font-family: var(--%NS%mat-chip-label-text-font, var(--%NS%mat-sys-label-large-font));
  line-height: var(--%NS%mat-chip-label-text-line-height, var(--%NS%mat-sys-label-large-line-height));
  font-size: var(--%NS%mat-chip-label-text-size, var(--%NS%mat-sys-label-large-size));
  font-weight: var(--%NS%mat-chip-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  letter-spacing: var(--%NS%mat-chip-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {
  color: var(--%NS%mat-chip-label-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--%NS%selected:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {
  color: var(--%NS%mat-chip-selected-label-text-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label, .mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label {
  color: var(--%NS%mat-chip-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mdc-evolution-chip__graphic {
  align-items: center;
  display: inline-flex;
  justify-content: center;
  overflow: hidden;
  pointer-events: none;
  position: relative;
  flex: 1 0 auto;
}
.mat-mdc-standard-chip .mdc-evolution-chip__graphic {
  width: var(--%NS%mat-chip-with-avatar-avatar-size, 24px);
  height: var(--%NS%mat-chip-with-avatar-avatar-size, 24px);
  font-size: var(--%NS%mat-chip-with-avatar-avatar-size, 24px);
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__graphic {
  transition: width 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mdc-evolution-chip--%NS%selectable:not(.mdc-evolution-chip--selected):not(.mdc-evolution-chip--with-primary-icon) .mdc-evolution-chip__graphic {
  width: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 6px;
  padding-right: 6px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 4px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 8px;
  padding-right: 4px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 6px;
  padding-right: 6px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 4px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 8px;
  padding-right: 4px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__graphic {
  padding-left: 0;
}

.mdc-evolution-chip__checkmark {
  position: absolute;
  opacity: 0;
  top: 50%;
  left: 50%;
  height: 20px;
  width: 20px;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__checkmark {
  color: var(--%NS%mat-chip-with-icon-selected-icon-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__checkmark {
  color: var(--%NS%mat-chip-with-icon-disabled-icon-color, var(--%NS%mat-sys-on-surface));
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark {
  transition: transform 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  transform: translate(-75%, -50%);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {
  transform: translate(-50%, -50%);
  opacity: 1;
}

.mdc-evolution-chip__checkmark-svg {
  display: block;
}

.mdc-evolution-chip__checkmark-path {
  stroke-width: 2px;
  stroke-dasharray: 29.7833385;
  stroke-dashoffset: 29.7833385;
  stroke: currentColor;
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark-path {
  transition: stroke-dashoffset 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark-path {
  stroke-dashoffset: 0;
}
@media (forced-colors: active) {
  .mdc-evolution-chip__checkmark-path {
    stroke: CanvasText !important;
  }
}

.mat-mdc-standard-chip .mdc-evolution-chip__icon--trailing {
  height: 18px;
  width: 18px;
  font-size: 18px;
}
.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove {
  opacity: calc(var(--%NS%mat-chip-trailing-action-opacity, 1) * var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));
}
.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove:focus {
  opacity: calc(var(--%NS%mat-chip-trailing-action-focus-opacity, 1) * var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));
}

.mat-mdc-standard-chip {
  border-radius: var(--%NS%mat-chip-container-shape-radius, 8px);
  height: var(--%NS%mat-chip-container-height, 32px);
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) {
  background-color: var(--%NS%mat-chip-elevated-container-color, transparent);
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled {
  background-color: var(--%NS%mat-chip-elevated-disabled-container-color);
}
.mat-mdc-standard-chip.mdc-evolution-chip--%NS%selected:not(.mdc-evolution-chip--disabled) {
  background-color: var(--%NS%mat-chip-elevated-selected-container-color, var(--%NS%mat-sys-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled {
  background-color: var(--%NS%mat-chip-flat-disabled-selected-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-standard-chip {
    outline: solid 1px;
  }
}

.mat-mdc-standard-chip .mdc-evolution-chip__icon--primary {
  border-radius: var(--%NS%mat-chip-with-avatar-avatar-shape-radius, 24px);
  width: var(--%NS%mat-chip-with-icon-icon-size, 18px);
  height: var(--%NS%mat-chip-with-icon-icon-size, 18px);
  font-size: var(--%NS%mat-chip-with-icon-icon-size, 18px);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__icon--primary {
  opacity: 0;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__icon--primary {
  color: var(--%NS%mat-chip-with-icon-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--primary {
  color: var(--%NS%mat-chip-with-icon-disabled-icon-color, var(--%NS%mat-sys-on-surface));
}

.mat-mdc-chip-highlighted {
  --%NS%mat-chip-with-icon-icon-color: var(--%NS%mat-chip-with-icon-selected-icon-color, var(--%NS%mat-sys-on-secondary-container));
  --%NS%mat-chip-elevated-container-color: var(--%NS%mat-chip-elevated-selected-container-color, var(--%NS%mat-sys-secondary-container));
  --%NS%mat-chip-label-text-color: var(--%NS%mat-chip-selected-label-text-color, var(--%NS%mat-sys-on-secondary-container));
  --%NS%mat-chip-outline-width: var(--%NS%mat-chip-flat-selected-outline-width, 0);
}

.mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-focus-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-chip-selected .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted .mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-selected-focus-state-layer-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-mdc-chip:hover .mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-hover-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
  opacity: var(--%NS%mat-chip-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-chip-focus-overlay .mat-mdc-chip-selected:hover, .mat-mdc-chip-highlighted:hover .mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-selected-hover-state-layer-color, var(--%NS%mat-sys-on-secondary-container));
  opacity: var(--%NS%mat-chip-selected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-chip.cdk-focused .mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-focus-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
  opacity: var(--%NS%mat-chip-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-chip-selected.cdk-focused .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted.cdk-focused .mat-mdc-chip-focus-overlay {
  background: var(--%NS%mat-chip-selected-focus-state-layer-color, var(--%NS%mat-sys-on-secondary-container));
  opacity: var(--%NS%mat-chip-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}

.mdc-evolution-chip--%NS%disabled:not(.mdc-evolution-chip--selected) .mat-mdc-chip-avatar {
  opacity: var(--%NS%mat-chip-with-avatar-disabled-avatar-opacity, 0.38);
}

.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {
  opacity: var(--%NS%mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38);
}

.mdc-evolution-chip--disabled.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {
  opacity: var(--%NS%mat-chip-with-icon-disabled-icon-opacity, 0.38);
}

.mat-mdc-standard-chip.mdc-evolution-chip--disabled {
  opacity: var(--%NS%mat-chip-disabled-container-opacity, 1);
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted .mdc-evolution-chip__icon--trailing {
  color: var(--%NS%mat-chip-selected-trailing-icon-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {
  color: var(--%NS%mat-chip-selected-disabled-trailing-icon-color, var(--%NS%mat-sys-on-surface));
}

.mat-mdc-chip-edit, .mat-mdc-chip-remove {
  opacity: var(--%NS%mat-chip-trailing-action-opacity, 1);
}
.mat-mdc-chip-edit:focus, .mat-mdc-chip-remove:focus {
  opacity: var(--%NS%mat-chip-trailing-action-focus-opacity, 1);
}
.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {
  background-color: var(--%NS%mat-chip-trailing-action-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-chip-edit:hover::after, .mat-mdc-chip-remove:hover::after {
  opacity: calc(var(--%NS%mat-chip-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)));
}
.mat-mdc-chip-edit:focus::after, .mat-mdc-chip-remove:focus::after {
  opacity: calc(var(--%NS%mat-chip-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)));
}

.mat-mdc-chip-selected .mat-mdc-chip-remove::after,
.mat-mdc-chip-highlighted .mat-mdc-chip-remove::after {
  background-color: var(--%NS%mat-chip-selected-trailing-action-state-layer-color, var(--%NS%mat-sys-on-secondary-container));
}

.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:focus::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:focus::after {
  opacity: calc(var(--%NS%mat-chip-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)));
}
.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:hover::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:hover::after {
  opacity: calc(var(--%NS%mat-chip-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity)) + var(--%NS%mat-chip-trailing-action-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity)));
}

.mat-mdc-standard-chip {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-standard-chip .mat-mdc-chip-graphic,
.mat-mdc-standard-chip .mat-mdc-chip-trailing-icon {
  box-sizing: content-box;
}
.mat-mdc-standard-chip._mat-animation-noopable,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__graphic,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark-path {
  transition-duration: 1ms;
  animation-duration: 1ms;
}

.mat-mdc-chip-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  opacity: 0;
  border-radius: inherit;
  transition: opacity 150ms linear;
}
._mat-animation-noopable .mat-mdc-chip-focus-overlay {
  transition: none;
}
.mat-mdc-basic-chip .mat-mdc-chip-focus-overlay {
  display: none;
}

.mat-mdc-chip .mat-ripple.mat-mdc-chip-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}

.mat-mdc-chip-avatar {
  text-align: center;
  line-height: 1;
  color: var(--%NS%mat-chip-with-icon-icon-color, currentColor);
}

.mat-mdc-chip {
  position: relative;
  z-index: 0;
}

.mat-mdc-chip-action-label {
  text-align: left;
  z-index: 1;
}
[dir=rtl] .mat-mdc-chip-action-label {
  text-align: right;
}
.mat-mdc-chip.mdc-evolution-chip--with-trailing-action .mat-mdc-chip-action-label {
  position: relative;
}
.mat-mdc-chip-action-label .mat-mdc-chip-primary-focus-indicator {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  pointer-events: none;
}
.mat-mdc-chip-action-label .mat-focus-indicator::before {
  margin: calc(calc(var(--%NS%mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-chip-edit::before, .mat-mdc-chip-remove::before {
  margin: calc(var(--%NS%mat-focus-indicator-border-width, 3px) * -1);
  left: 8px;
  right: 8px;
}
.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {
  content: "";
  display: block;
  opacity: 0;
  position: absolute;
  top: -3px;
  bottom: -3px;
  left: 5px;
  right: 5px;
  border-radius: 50%;
  box-sizing: border-box;
  padding: 12px;
  margin: -12px;
  background-clip: content-box;
}
.mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {
  width: 18px;
  height: 18px;
  font-size: 18px;
  box-sizing: content-box;
}

.mat-chip-edit-input {
  cursor: text;
  display: inline-block;
  color: inherit;
  outline: 0;
}

@media (forced-colors: active) {
  .mat-mdc-chip-selected:not(.mat-mdc-chip-multiple) {
    outline-width: 3px;
  }
}

.mat-mdc-chip-action:focus-visible .mat-focus-indicator::before {
  content: "";
}

.mdc-evolution-chip__icon, .mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {
  min-height: fit-content;
}

img.mdc-evolution-chip__icon {
  min-height: 0;
}
`],encapsulation:2})})()}return t})();var aN=(()=>{class t{_elementRef=m(L);_changeDetectorRef=m(Ie);_dir=m(Mt,{optional:!0});_lastDestroyedFocusedChipIndex=null;_keyManager;_destroyed=new E;_defaultRole="presentation";get chipFocusChanges(){return this._getChipStream(e=>e._onFocus)}get chipDestroyedChanges(){return this._getChipStream(e=>e.destroyed)}get chipRemovedChanges(){return this._getChipStream(e=>e.removed)}get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._syncChipsState()}_disabled=!1;get empty(){return!this._chips||this._chips.length===0}get role(){return this._explicitRole?this._explicitRole:this.empty?null:this._defaultRole}tabIndex=0;set role(e){this._explicitRole=e}_explicitRole=null;get focused(){return this._hasFocusedChip()}_chips;_chipActions=new nn;ngAfterViewInit(){this._setUpFocusManagement(),this._trackChipSetChanges(),this._trackDestroyedFocusedChip()}ngOnDestroy(){this._keyManager?.destroy(),this._chipActions.destroy(),this._destroyed.next(),this._destroyed.complete()}_hasFocusedChip(){return this._chips&&this._chips.some(e=>e._hasFocus())}_syncChipsState(){this._chips?.forEach(e=>{e._chipListDisabled=this._disabled,e._changeDetectorRef.markForCheck()})}focus(){}_handleKeydown(e){this._originatesFromChip(e)&&this._keyManager.onKeydown(e)}_isValidIndex(e){return e>=0&&e<this._chips.length}_allowFocusEscape(){let e=this._elementRef.nativeElement.tabIndex;e!==-1&&(this._elementRef.nativeElement.tabIndex=-1,setTimeout(()=>this._elementRef.nativeElement.tabIndex=e))}_getChipStream(e){return this._chips.changes.pipe(yt(null),pa(()=>jt(...this._chips.map(e))))}_originatesFromChip(e){let i=e.target;for(;i&&i!==this._elementRef.nativeElement;){if(i.classList.contains("mat-mdc-chip"))return!0;i=i.parentElement}return!1}_setUpFocusManagement(){this._chips.changes.pipe(yt(this._chips)).subscribe(e=>{let i=[];e.forEach(a=>a._getActions().forEach(r=>i.push(r))),this._chipActions.reset(i),this._chipActions.notifyOnChanges()}),this._keyManager=new Ji(this._chipActions).withVerticalOrientation().withHorizontalOrientation(this._dir?this._dir.value:"ltr").withHomeAndEnd().skipPredicate(e=>this._skipPredicate(e)),this.chipFocusChanges.pipe(Oe(this._destroyed)).subscribe(({chip:e})=>{let i=e._getSourceAction(document.activeElement);i&&this._keyManager.updateActiveItem(i)}),this._dir?.change.pipe(Oe(this._destroyed)).subscribe(e=>this._keyManager.withHorizontalOrientation(e))}_skipPredicate(e){return e.disabled}_trackChipSetChanges(){this._chips.changes.pipe(yt(null),Oe(this._destroyed)).subscribe(()=>{this.disabled&&Promise.resolve().then(()=>this._syncChipsState()),this._redirectDestroyedChipFocus()})}_trackDestroyedFocusedChip(){this.chipDestroyedChanges.pipe(Oe(this._destroyed)).subscribe(e=>{let a=this._chips.toArray().indexOf(e.chip),r=e.chip._hasFocus(),o=e.chip._hadFocusOnRemove&&this._keyManager.activeItem&&e.chip._getActions().includes(this._keyManager.activeItem),s=r||o;this._isValidIndex(a)&&s&&(this._lastDestroyedFocusedChipIndex=a)})}_redirectDestroyedChipFocus(){if(this._lastDestroyedFocusedChipIndex!=null){if(this._chips.length){let e=Math.min(this._lastDestroyedFocusedChipIndex,this._chips.length-1),i=this._chips.toArray()[e];i.disabled?this._chips.length===1?this.focus():this._keyManager.setPreviousItemActive():i.focus()}else this.focus();this._lastDestroyedFocusedChipIndex=null}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){return B({type:t,selectors:[["mat-chip-set"]],contentQueries:function(a,r,o){if(a&1&&Ye(o,Tp,5),a&2){let s;M(s=T())&&(r._chips=s)}},hostAttrs:[1,"mat-mdc-chip-set","mdc-evolution-chip-set"],hostVars:1,hostBindings:function(a,r){a&1&&C("keydown",function(s){return r._handleKeydown(s)}),a&2&&K("role",r.role)},inputs:{disabled:[2,"disabled","disabled",I],role:"role",tabIndex:[2,"tabIndex","tabIndex",i=>i==null?0:ht(i)]},ngContentSelectors:["*"],decls:2,vars:0,consts:[["role","presentation",1,"mdc-evolution-chip-set__chips"]],template:function(a,r){a&1&&(he(),We(0,"div",0),V(1),qe())},styles:[`.mat-mdc-chip-set {
  display: flex;
}
.mat-mdc-chip-set:focus {
  outline: none;
}
.mat-mdc-chip-set .mdc-evolution-chip-set__chips {
  min-width: 100%;
  margin-left: -8px;
  margin-right: 0;
}
.mat-mdc-chip-set .mdc-evolution-chip {
  margin: 8px 0 8px 8px;
}
[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip-set__chips {
  margin-left: 0;
  margin-right: -8px;
}
[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip {
  margin-left: 0;
  margin-right: 8px;
}

.mdc-evolution-chip-set__chips {
  display: flex;
  flex-flow: wrap;
  min-width: 0;
}

.mat-mdc-chip-set-stacked {
  flex-direction: column;
  align-items: flex-start;
}
.mat-mdc-chip-set-stacked .mat-mdc-chip {
  width: 100%;
}
.mat-mdc-chip-set-stacked .mdc-evolution-chip__graphic {
  flex-grow: 0;
}
.mat-mdc-chip-set-stacked .mdc-evolution-chip__action--primary {
  flex-basis: 100%;
  justify-content: start;
}

input.mat-mdc-chip-input {
  flex: 1 0 150px;
  margin-left: 8px;
}
[dir=rtl] input.mat-mdc-chip-input {
  margin-left: 0;
  margin-right: 8px;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-moz-placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-webkit-input-placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input:-ms-input-placeholder {
  opacity: 1;
}
.mat-mdc-chip-set + input.mat-mdc-chip-input {
  margin-left: 0;
  margin-right: 0;
}
`],encapsulation:2})})()}return t})(),Ap=class{source;value;constructor(n,e){this.source=n,this.value=e}},rN={provide:Cn,useExisting:ze(()=>Lp),multi:!0},Lp=(()=>{class t extends aN{_onTouched=()=>{};_onChange=()=>{};_defaultRole="listbox";_defaultOptions=m(Rp,{optional:!0});get multiple(){return this._multiple}set multiple(e){this._multiple=e,this._syncListboxProperties()}_multiple=!1;get selected(){let e=this._chips.toArray().filter(i=>i.selected);return this.multiple?e:e[0]}ariaOrientation="horizontal";get selectable(){return this._selectable}set selectable(e){this._selectable=e,this._syncListboxProperties()}_selectable=!0;compareWith=(e,i)=>e===i;required=!1;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._syncListboxProperties()}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get chipSelectionChanges(){return this._getChipStream(e=>e.selectionChange)}get chipBlurChanges(){return this._getChipStream(e=>e._onBlur)}get value(){return this._value}set value(e){this._chips&&this._chips.length&&this._setSelectionByValue(e,!1),this._value=e}_value;change=new z;_chips=void 0;ngAfterContentInit(){this._chips.changes.pipe(yt(null),Oe(this._destroyed)).subscribe(()=>{this.value!==void 0&&Promise.resolve().then(()=>{this._setSelectionByValue(this.value,!1)}),this._syncListboxProperties()}),this.chipBlurChanges.pipe(Oe(this._destroyed)).subscribe(()=>this._blur()),this.chipSelectionChanges.pipe(Oe(this._destroyed)).subscribe(e=>{this.multiple||this._chips.forEach(i=>{i!==e.source&&i._setSelectedState(!1,!1,!1)}),e.isUserInput&&this._propagateChanges()})}focus(){if(this.disabled)return;let e=this._getFirstSelectedChip();e&&!e.disabled?e.focus():this._chips.length>0?this._keyManager.setFirstItemActive():this._elementRef.nativeElement.focus()}writeValue(e){e!=null?this.value=e:this.value=void 0}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e}_setSelectionByValue(e,i=!0){this._clearSelection(),Array.isArray(e)?e.forEach(a=>this._selectValue(a,i)):this._selectValue(e,i)}_blur(){this.disabled||setTimeout(()=>{this.focused||this._markAsTouched()})}_keydown(e){e.keyCode===9&&super._allowFocusEscape()}_markAsTouched(){this._onTouched(),this._changeDetectorRef.markForCheck()}_propagateChanges(){let e=null;Array.isArray(this.selected)?e=this.selected.map(i=>i.value):e=this.selected?this.selected.value:void 0,this._value=e,this.change.emit(new Ap(this,e)),this._onChange(e),this._changeDetectorRef.markForCheck()}_clearSelection(e){this._chips.forEach(i=>{i!==e&&i.deselect()})}_selectValue(e,i){let a=this._chips.find(r=>r.value!=null&&this.compareWith(r.value,e));return a&&(i?a.selectViaInteraction():a.select()),a}_syncListboxProperties(){this._chips&&Promise.resolve().then(()=>{this._chips.forEach(e=>{e._chipListMultiple=this.multiple,e.chipListSelectable=this._selectable,e._chipListHideSingleSelectionIndicator=this.hideSingleSelectionIndicator,e._changeDetectorRef.markForCheck()})})}_getFirstSelectedChip(){return Array.isArray(this.selected)?this.selected.length?this.selected[0]:void 0:this.selected}_skipPredicate(e){return!1}static \u0275fac=(()=>{let e;return function(a){return(e||(e=Fe(t)))(a||t)}})();static \u0275cmp=(function(){let e=["*"];return B({type:t,selectors:[["mat-chip-listbox"]],contentQueries:function(a,r,o){if(a&1&&Ye(o,Fp,5),a&2){let s;M(s=T())&&(r._chips=s)}},hostAttrs:[1,"mdc-evolution-chip-set","mat-mdc-chip-listbox"],hostVars:10,hostBindings:function(a,r){a&1&&C("focus",function(){return r.focus()})("blur",function(){return r._blur()})("keydown",function(s){return r._keydown(s)}),a&2&&(Gt("tabIndex",r.disabled||r.empty?-1:r.tabIndex),K("role",r.role)("aria-required",r.role?r.required:null)("aria-disabled",r.disabled.toString())("aria-multiselectable",r.multiple)("aria-orientation",r.ariaOrientation),S("mat-mdc-chip-list-disabled",r.disabled)("mat-mdc-chip-list-required",r.required))},inputs:{multiple:[2,"multiple","multiple",I],ariaOrientation:[0,"aria-orientation","ariaOrientation"],selectable:[2,"selectable","selectable",I],compareWith:"compareWith",required:[2,"required","required",I],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",I],value:"value"},outputs:{change:"change"},features:[_e([rN]),le],ngContentSelectors:e,decls:2,vars:0,consts:[["role","presentation",1,"mdc-evolution-chip-set__chips"]],template:function(a,r){a&1&&(he(),We(0,"div",0),V(1),qe())},styles:[`.mat-mdc-chip-set {
  display: flex;
}
.mat-mdc-chip-set:focus {
  outline: none;
}
.mat-mdc-chip-set .mdc-evolution-chip-set__chips {
  min-width: 100%;
  margin-left: -8px;
  margin-right: 0;
}
.mat-mdc-chip-set .mdc-evolution-chip {
  margin: 8px 0 8px 8px;
}
[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip-set__chips {
  margin-left: 0;
  margin-right: -8px;
}
[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip {
  margin-left: 0;
  margin-right: 8px;
}

.mdc-evolution-chip-set__chips {
  display: flex;
  flex-flow: wrap;
  min-width: 0;
}

.mat-mdc-chip-set-stacked {
  flex-direction: column;
  align-items: flex-start;
}
.mat-mdc-chip-set-stacked .mat-mdc-chip {
  width: 100%;
}
.mat-mdc-chip-set-stacked .mdc-evolution-chip__graphic {
  flex-grow: 0;
}
.mat-mdc-chip-set-stacked .mdc-evolution-chip__action--primary {
  flex-basis: 100%;
  justify-content: start;
}

input.mat-mdc-chip-input {
  flex: 1 0 150px;
  margin-left: 8px;
}
[dir=rtl] input.mat-mdc-chip-input {
  margin-left: 0;
  margin-right: 8px;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-moz-placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-webkit-input-placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input:-ms-input-placeholder {
  opacity: 1;
}
.mat-mdc-chip-set + input.mat-mdc-chip-input {
  margin-left: 0;
  margin-right: 0;
}
`],encapsulation:2})})()}return t})();var Ly=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=G({providers:[il,{provide:Rp,useValue:{separatorKeyCodes:[13]}}],imports:[Sn,me]})}return t})();var oo=class{_multiple;_emitChanges;compareWith;_selection=new Set;_deselectedToEmit=[];_selectedToEmit=[];_selected=null;get selected(){return this._selected||(this._selected=Array.from(this._selection.values())),this._selected}changed=new E;bulk={select:n=>this._select(n),deselect:n=>this._deselect(n),setSelection:n=>this._setSelection(n)};constructor(n=!1,e,i=!0,a){this._multiple=n,this._emitChanges=i,this.compareWith=a,e&&e.length&&(n?e.forEach(r=>this._markSelected(r)):this._markSelected(e[0]),this._selectedToEmit.length=0)}select(...n){return this._select(n)}deselect(...n){return this._deselect(n)}setSelection(...n){return this._setSelection(n)}toggle(n){return this.isSelected(n)?this.deselect(n):this.select(n)}clear(n=!0){this._unmarkAll();let e=this._hasQueuedChanges();return n&&this._emitChangeEvent(),e}isSelected(n){return this._selection.has(this._getConcreteValue(n))}isEmpty(){return this._selection.size===0}hasValue(){return!this.isEmpty()}sort(n){this._multiple&&this.selected&&this._selected.sort(n)}isMultipleSelection(){return this._multiple}_select(n){this._verifyValueAssignment(n),n.forEach(i=>this._markSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}_deselect(n){this._verifyValueAssignment(n),n.forEach(i=>this._unmarkSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}_setSelection(n){this._verifyValueAssignment(n);let e=this.selected,i=new Set(n.map(r=>this._getConcreteValue(r)));n.forEach(r=>this._markSelected(r)),e.filter(r=>!i.has(this._getConcreteValue(r,i))).forEach(r=>this._unmarkSelected(r));let a=this._hasQueuedChanges();return this._emitChangeEvent(),a}_emitChangeEvent(){this._selected=null,(this._selectedToEmit.length||this._deselectedToEmit.length)&&(this.changed.next({source:this,added:this._selectedToEmit,removed:this._deselectedToEmit}),this._deselectedToEmit=[],this._selectedToEmit=[])}_markSelected(n){n=this._getConcreteValue(n),this.isSelected(n)||(this._multiple||this._unmarkAll(),this.isSelected(n)||this._selection.add(n),this._emitChanges&&this._selectedToEmit.push(n))}_unmarkSelected(n){n=this._getConcreteValue(n),this.isSelected(n)&&(this._selection.delete(n),this._emitChanges&&this._deselectedToEmit.push(n))}_unmarkAll(){this.isEmpty()||this._selection.forEach(n=>this._unmarkSelected(n))}_verifyValueAssignment(n){n.length>1&&this._multiple}_hasQueuedChanges(){return!!(this._deselectedToEmit.length||this._selectedToEmit.length)}_getConcreteValue(n,e){if(this.compareWith){e=e??this._selection;for(let i of e)if(this.compareWith(n,i))return i;return n}else return n}};var Pp=(()=>{class t{_listeners=[];notify(e,i){for(let a of this._listeners)a(e,i)}listen(e){return this._listeners.push(e),()=>{this._listeners=this._listeners.filter(i=>e!==i)}}ngOnDestroy(){this._listeners=[]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})}return t})();var Py=(()=>{class t{_animationsDisabled=Re();state="unchecked";disabled=!1;appearance="full";static \u0275fac=function(i){return new(i||t)};static \u0275cmp=B({type:t,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(i,a){i&2&&S("mat-pseudo-checkbox-indeterminate",a.state==="indeterminate")("mat-pseudo-checkbox-checked",a.state==="checked")("mat-pseudo-checkbox-disabled",a.disabled)("mat-pseudo-checkbox-minimal",a.appearance==="minimal")("mat-pseudo-checkbox-full",a.appearance==="full")("_mat-animation-noopable",a._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(i,a){},styles:[`.mat-pseudo-checkbox {
  border-radius: 2px;
  cursor: pointer;
  display: inline-block;
  vertical-align: middle;
  box-sizing: border-box;
  position: relative;
  flex-shrink: 0;
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 0.1), background-color 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox::after {
  position: absolute;
  opacity: 0;
  content: "";
  border-bottom: 2px solid currentColor;
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-pseudo-checkbox._mat-animation-noopable::after {
  transition: none;
}

.mat-pseudo-checkbox-disabled {
  cursor: default;
}

.mat-pseudo-checkbox-indeterminate::after {
  left: 1px;
  opacity: 1;
  border-radius: 2px;
}

.mat-pseudo-checkbox-checked::after {
  left: 1px;
  border-left: 2px solid currentColor;
  transform: rotate(-45deg);
  opacity: 1;
  box-sizing: content-box;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  color: var(--%NS%mat-pseudo-checkbox-minimal-selected-checkmark-color, var(--%NS%mat-sys-primary));
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--%NS%mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mat-pseudo-checkbox-full {
  border-color: var(--%NS%mat-pseudo-checkbox-full-unselected-icon-color, var(--%NS%mat-sys-on-surface-variant));
  border-width: 2px;
  border-style: solid;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-disabled {
  border-color: var(--%NS%mat-pseudo-checkbox-full-disabled-unselected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate {
  background-color: var(--%NS%mat-pseudo-checkbox-full-selected-icon-color, var(--%NS%mat-sys-primary));
  border-color: transparent;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  color: var(--%NS%mat-pseudo-checkbox-full-selected-checkmark-color, var(--%NS%mat-sys-on-primary));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled {
  background-color: var(--%NS%mat-pseudo-checkbox-full-disabled-selected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--%NS%mat-pseudo-checkbox-full-disabled-selected-checkmark-color, var(--%NS%mat-sys-surface));
}

.mat-pseudo-checkbox {
  width: 18px;
  height: 18px;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after {
  width: 14px;
  height: 6px;
  transform-origin: center;
  top: -4.2426406871px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  top: 8px;
  width: 16px;
}

.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after {
  width: 10px;
  height: 4px;
  transform-origin: center;
  top: -2.8284271247px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  top: 6px;
  width: 12px;
}
`],encapsulation:2})}return t})();var Vy=new y("MAT_BUTTON_TOGGLE_DEFAULT_OPTIONS",{providedIn:"root",factory:()=>({hideSingleSelectionIndicator:!1,hideMultipleSelectionIndicator:!1,disabledInteractive:!1})}),By=new y("MatButtonToggleGroup"),sN={provide:Cn,useExisting:ze(()=>Vp),multi:!0},ll=class{source;value;constructor(n,e){this.source=n,this.value=e}},Vp=(()=>{class t{_changeDetector=m(Ie);_dir=m(Mt,{optional:!0});_multiple=!1;_disabled=!1;_disabledInteractive=!1;_selectionModel;_rawValue;_controlValueAccessorChangeFn=()=>{};_onTouched=()=>{};_buttonToggles;appearance;get name(){return this._name}set name(e){this._name=e,this._markButtonsForCheck()}_name=m(Pe).getId("mat-button-toggle-group-");vertical=!1;get value(){let e=this._selectionModel?this._selectionModel.selected:[];return this.multiple?e.map(i=>i.value):e[0]?e[0].value:void 0}set value(e){this._setSelectionByValue(e),this.valueChange.emit(this.value)}valueChange=new z;get selected(){let e=this._selectionModel?this._selectionModel.selected:[];return this.multiple?e:e[0]||null}get multiple(){return this._multiple}set multiple(e){this._multiple=e,this._markButtonsForCheck()}get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._markButtonsForCheck()}get disabledInteractive(){return this._disabledInteractive}set disabledInteractive(e){this._disabledInteractive=e,this._markButtonsForCheck()}get dir(){return this._dir&&this._dir.value==="rtl"?"rtl":"ltr"}change=new z;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._markButtonsForCheck()}_hideSingleSelectionIndicator;get hideMultipleSelectionIndicator(){return this._hideMultipleSelectionIndicator}set hideMultipleSelectionIndicator(e){this._hideMultipleSelectionIndicator=e,this._markButtonsForCheck()}_hideMultipleSelectionIndicator;constructor(){let e=m(Vy,{optional:!0});this.appearance=e&&e.appearance?e.appearance:"standard",this._hideSingleSelectionIndicator=e?.hideSingleSelectionIndicator??!1,this._hideMultipleSelectionIndicator=e?.hideMultipleSelectionIndicator??!1}ngOnInit(){this._selectionModel=new oo(this.multiple,void 0,!1)}ngAfterContentInit(){this._selectionModel.select(...this._buttonToggles.filter(e=>e.checked)),this.multiple||this._initializeTabIndex()}writeValue(e){this.value=e,this._changeDetector.markForCheck()}registerOnChange(e){this._controlValueAccessorChangeFn=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e}_keydown(e){if(this.multiple||this.disabled||$a(e))return;let a=e.target.id,r=this._buttonToggles.toArray().findIndex(s=>s.buttonId===a),o=null;switch(e.keyCode){case 32:case 13:o=this._buttonToggles.get(r)||null;break;case 38:o=this._getNextButton(r,-1);break;case 37:o=this._getNextButton(r,this.dir==="ltr"?-1:1);break;case 40:o=this._getNextButton(r,1);break;case 39:o=this._getNextButton(r,this.dir==="ltr"?1:-1);break;default:return}o&&(e.preventDefault(),o._onButtonClick(),o.focus())}_emitChangeEvent(e){let i=new ll(e,this.value);this._rawValue=i.value,this._controlValueAccessorChangeFn(i.value),this.change.emit(i)}_syncButtonToggle(e,i,a=!1,r=!1){!this.multiple&&this.selected&&!e.checked&&(this.selected.checked=!1),this._selectionModel?i?this._selectionModel.select(e):this._selectionModel.deselect(e):r=!0,r?Promise.resolve().then(()=>this._updateModelValue(e,a)):this._updateModelValue(e,a)}_isSelected(e){return this._selectionModel&&this._selectionModel.isSelected(e)}_isPrechecked(e){return typeof this._rawValue>"u"?!1:this.multiple&&Array.isArray(this._rawValue)?this._rawValue.some(i=>e.value!=null&&i===e.value):e.value===this._rawValue}_initializeTabIndex(){if(this._buttonToggles.forEach(e=>{e.tabIndex=-1}),this.selected)this.selected.tabIndex=0;else for(let e=0;e<this._buttonToggles.length;e++){let i=this._buttonToggles.get(e);if(!i.disabled){i.tabIndex=0;break}}}_getNextButton(e,i){let a=this._buttonToggles;for(let r=1;r<=a.length;r++){let o=(e+i*r+a.length)%a.length,s=a.get(o);if(s&&!s.disabled)return s}return null}_setSelectionByValue(e){if(this._rawValue=e,!this._buttonToggles)return;let i=this._buttonToggles.toArray();if(this.multiple&&e?(Array.isArray(e),this._clearSelection(),e.forEach(a=>this._selectValue(a,i))):(this._clearSelection(),this._selectValue(e,i)),!this.multiple&&i.every(a=>a.tabIndex===-1)){for(let a of i)if(!a.disabled){a.tabIndex=0;break}}}_clearSelection(){this._selectionModel.clear(),this._buttonToggles.forEach(e=>{e.checked=!1,this.multiple||(e.tabIndex=-1)})}_selectValue(e,i){for(let a of i)if(a.value===e){a.checked=!0,this._selectionModel.select(a),this.multiple||(a.tabIndex=0);break}}_updateModelValue(e,i){i&&this._emitChangeEvent(e),this.valueChange.emit(this.value)}_markButtonsForCheck(){this._buttonToggles?.forEach(e=>e._markForCheck())}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["mat-button-toggle-group"]],contentQueries:function(i,a,r){if(i&1&&Ye(r,dl,5),i&2){let o;M(o=T())&&(a._buttonToggles=o)}},hostAttrs:[1,"mat-button-toggle-group"],hostVars:6,hostBindings:function(i,a){i&1&&C("keydown",function(o){return a._keydown(o)}),i&2&&(K("role",a.multiple?"group":"radiogroup")("aria-disabled",a.disabled),S("mat-button-toggle-vertical",a.vertical)("mat-button-toggle-group-appearance-standard",a.appearance==="standard"))},inputs:{appearance:"appearance",name:"name",vertical:[2,"vertical","vertical",I],value:"value",multiple:[2,"multiple","multiple",I],disabled:[2,"disabled","disabled",I],disabledInteractive:[2,"disabledInteractive","disabledInteractive",I],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",I],hideMultipleSelectionIndicator:[2,"hideMultipleSelectionIndicator","hideMultipleSelectionIndicator",I]},outputs:{valueChange:"valueChange",change:"change"},exportAs:["matButtonToggleGroup"],features:[_e([sN,{provide:By,useExisting:t}])]})}return t})(),dl=(()=>{class t{_changeDetectorRef=m(Ie);_elementRef=m(L);_focusMonitor=m(on);_idGenerator=m(Pe);_animationDisabled=Re();_checked=!1;ariaLabel;ariaLabelledby=null;_buttonElement;buttonToggleGroup;get buttonId(){return`${this.id}-button`}id;name;value;get tabIndex(){return this._tabIndex()}set tabIndex(e){this._tabIndex.set(e)}_tabIndex;disableRipple=!1;get appearance(){return this.buttonToggleGroup?this.buttonToggleGroup.appearance:this._appearance}set appearance(e){this._appearance=e}_appearance;get checked(){return this.buttonToggleGroup?this.buttonToggleGroup._isSelected(this):this._checked}set checked(e){e!==this._checked&&(this._checked=e,this.buttonToggleGroup&&this.buttonToggleGroup._syncButtonToggle(this,this._checked),this._changeDetectorRef.markForCheck())}get disabled(){return this._disabled||this.buttonToggleGroup&&this.buttonToggleGroup.disabled}set disabled(e){this._disabled=e}_disabled=!1;get disabledInteractive(){return this._disabledInteractive||this.buttonToggleGroup!==null&&this.buttonToggleGroup.disabledInteractive}set disabledInteractive(e){this._disabledInteractive=e}_disabledInteractive;change=new z;constructor(){m(je).load(Ct);let e=m(By,{optional:!0}),i=m(new Un("tabindex"),{optional:!0})||"",a=m(Vy,{optional:!0});this._tabIndex=ae(parseInt(i)||0),this.buttonToggleGroup=e,this._appearance=a&&a.appearance?a.appearance:"standard",this._disabledInteractive=a?.disabledInteractive??!1}ngOnInit(){let e=this.buttonToggleGroup;this.id=this.id||this._idGenerator.getId("mat-button-toggle-"),e&&(e._isPrechecked(this)?this.checked=!0:e._isSelected(this)!==this._checked&&e._syncButtonToggle(this,this._checked))}ngAfterViewInit(){this._animationDisabled||this._elementRef.nativeElement.classList.add("mat-button-toggle-animations-enabled"),this._focusMonitor.monitor(this._elementRef,!0)}ngOnDestroy(){let e=this.buttonToggleGroup;this._focusMonitor.stopMonitoring(this._elementRef),e&&e._isSelected(this)&&e._syncButtonToggle(this,!1,!1,!0)}focus(e){this._buttonElement.nativeElement.focus(e)}_onButtonClick(){if(this.disabled)return;let e=this.isSingleSelector()?!0:!this._checked;if(e!==this._checked&&(this._checked=e,this.buttonToggleGroup&&(this.buttonToggleGroup._syncButtonToggle(this,this._checked,!0),this.buttonToggleGroup._onTouched())),this.isSingleSelector()){let i=this.buttonToggleGroup._buttonToggles.find(a=>a.tabIndex===0);i&&(i.tabIndex=-1),this.tabIndex=0}this.change.emit(new ll(this,this.value))}_markForCheck(){this._changeDetectorRef.markForCheck()}_getButtonName(){return this.isSingleSelector()?this.buttonToggleGroup.name:this.name||null}isSingleSelector(){return this.buttonToggleGroup&&!this.buttonToggleGroup.multiple}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){let e=["button"],i=["*"];function a(r,o){if(r&1&&(p(0,"div",2),$(1,"mat-pseudo-checkbox",6),h()),r&2){let s=x();v(),F("disabled",s.disabled)}}return B({type:t,selectors:[["mat-button-toggle"]],viewQuery:function(o,s){if(o&1&&Be(e,5),o&2){let c;M(c=T())&&(s._buttonElement=c.first)}},hostAttrs:["role","presentation",1,"mat-button-toggle"],hostVars:14,hostBindings:function(o,s){o&1&&C("focus",function(){return s.focus()}),o&2&&(K("aria-label",null)("aria-labelledby",null)("id",s.id)("name",null),S("mat-button-toggle-standalone",!s.buttonToggleGroup)("mat-button-toggle-checked",s.checked)("mat-button-toggle-disabled",s.disabled)("mat-button-toggle-disabled-interactive",s.disabledInteractive)("mat-button-toggle-appearance-standard",s.appearance==="standard"))},inputs:{ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],id:"id",name:"name",value:"value",tabIndex:"tabIndex",disableRipple:[2,"disableRipple","disableRipple",I],appearance:"appearance",checked:[2,"checked","checked",I],disabled:[2,"disabled","disabled",I],disabledInteractive:[2,"disabledInteractive","disabledInteractive",I]},outputs:{change:"change"},exportAs:["matButtonToggle"],ngContentSelectors:i,decls:7,vars:13,consts:[["button",""],["type","button",1,"mat-button-toggle-button","mat-focus-indicator",3,"click","id","disabled"],[1,"mat-button-toggle-checkbox-wrapper"],[1,"mat-button-toggle-label-content"],[1,"mat-button-toggle-focus-overlay"],["matRipple","",1,"mat-button-toggle-ripple",3,"matRippleTrigger","matRippleDisabled"],["state","checked","aria-hidden","true","appearance","minimal",3,"disabled"]],template:function(o,s){if(o&1&&(he(),p(0,"button",1,0),C("click",function(){return s._onButtonClick()}),te(2,a,2,1,"div",2),p(3,"span",3),V(4),h()(),$(5,"span",4)(6,"span",5)),o&2){let c=et(1);F("id",s.buttonId)("disabled",s.disabled&&!s.disabledInteractive||null),K("role",s.isSingleSelector()?"radio":"button")("tabindex",s.disabled&&!s.disabledInteractive?-1:s.tabIndex)("aria-pressed",s.isSingleSelector()?null:s.checked)("aria-checked",s.isSingleSelector()?s.checked:null)("name",s._getButtonName())("aria-label",s.ariaLabel)("aria-labelledby",s.ariaLabelledby)("aria-disabled",s.disabled&&s.disabledInteractive?"true":null),v(2),ne(s.buttonToggleGroup&&(!s.buttonToggleGroup.multiple&&!s.buttonToggleGroup.hideSingleSelectionIndicator||s.buttonToggleGroup.multiple&&!s.buttonToggleGroup.hideMultipleSelectionIndicator)?2:-1),v(4),F("matRippleTrigger",c)("matRippleDisabled",s.disableRipple||s.disabled)}},dependencies:[Gn,Py],styles:[`.mat-button-toggle-standalone,
.mat-button-toggle-group {
  position: relative;
  display: inline-flex;
  flex-direction: row;
  white-space: nowrap;
  overflow: hidden;
  -webkit-tap-highlight-color: transparent;
  border-radius: var(--%NS%mat-button-toggle-legacy-shape);
  transform: translateZ(0);
}
.mat-button-toggle-standalone:not([class*=mat-elevation-z]),
.mat-button-toggle-group:not([class*=mat-elevation-z]) {
  box-shadow: 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12);
}
@media (forced-colors: active) {
  .mat-button-toggle-standalone,
  .mat-button-toggle-group {
    outline: solid 1px;
  }
}

.mat-button-toggle-standalone.mat-button-toggle-appearance-standard,
.mat-button-toggle-group-appearance-standard {
  border-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
  border: solid 1px var(--%NS%mat-button-toggle-divider-color, var(--%NS%mat-sys-outline));
}
.mat-button-toggle-standalone.mat-button-toggle-appearance-standard .mat-pseudo-checkbox,
.mat-button-toggle-group-appearance-standard .mat-pseudo-checkbox {
  --%NS%mat-pseudo-checkbox-minimal-selected-checkmark-color: var(--%NS%mat-button-toggle-selected-state-text-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-button-toggle-standalone.mat-button-toggle-appearance-standard:not([class*=mat-elevation-z]),
.mat-button-toggle-group-appearance-standard:not([class*=mat-elevation-z]) {
  box-shadow: none;
}
@media (forced-colors: active) {
  .mat-button-toggle-standalone.mat-button-toggle-appearance-standard,
  .mat-button-toggle-group-appearance-standard {
    outline: 0;
  }
}

.mat-button-toggle-vertical {
  flex-direction: column;
}
.mat-button-toggle-vertical .mat-button-toggle-label-content {
  display: block;
}

.mat-button-toggle {
  white-space: nowrap;
  position: relative;
  color: var(--%NS%mat-button-toggle-legacy-text-color);
  font-family: var(--%NS%mat-button-toggle-legacy-label-text-font);
  font-size: var(--%NS%mat-button-toggle-legacy-label-text-size);
  line-height: var(--%NS%mat-button-toggle-legacy-label-text-line-height);
  font-weight: var(--%NS%mat-button-toggle-legacy-label-text-weight);
  letter-spacing: var(--%NS%mat-button-toggle-legacy-label-text-tracking);
  --%NS%mat-pseudo-checkbox-minimal-selected-checkmark-color: var(--%NS%mat-button-toggle-legacy-selected-state-text-color);
}
.mat-button-toggle.cdk-keyboard-focused .mat-button-toggle-focus-overlay {
  opacity: var(--%NS%mat-button-toggle-legacy-focus-state-layer-opacity);
}
.mat-button-toggle .mat-icon svg {
  vertical-align: top;
}

.mat-button-toggle-checkbox-wrapper {
  display: inline-block;
  justify-content: flex-start;
  align-items: center;
  width: 0;
  height: 18px;
  line-height: 18px;
  overflow: hidden;
  box-sizing: border-box;
  position: absolute;
  top: 50%;
  left: 16px;
  transform: translate3d(0, -50%, 0);
}
[dir=rtl] .mat-button-toggle-checkbox-wrapper {
  left: auto;
  right: 16px;
}
.mat-button-toggle-appearance-standard .mat-button-toggle-checkbox-wrapper {
  left: 12px;
}
[dir=rtl] .mat-button-toggle-appearance-standard .mat-button-toggle-checkbox-wrapper {
  left: auto;
  right: 12px;
}
.mat-button-toggle-checked .mat-button-toggle-checkbox-wrapper {
  width: 18px;
}
.mat-button-toggle-animations-enabled .mat-button-toggle-checkbox-wrapper {
  transition: width 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-button-toggle-vertical .mat-button-toggle-checkbox-wrapper {
  transition: none;
}

.mat-button-toggle-checked {
  color: var(--%NS%mat-button-toggle-legacy-selected-state-text-color);
  background-color: var(--%NS%mat-button-toggle-legacy-selected-state-background-color);
}

.mat-button-toggle-disabled {
  pointer-events: none;
  color: var(--%NS%mat-button-toggle-legacy-disabled-state-text-color);
  background-color: var(--%NS%mat-button-toggle-legacy-disabled-state-background-color);
  --%NS%mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color: var(--%NS%mat-button-toggle-legacy-disabled-state-text-color);
}
.mat-button-toggle-disabled.mat-button-toggle-checked {
  background-color: var(--%NS%mat-button-toggle-legacy-disabled-selected-state-background-color);
}

.mat-button-toggle-disabled-interactive {
  pointer-events: auto;
}

.mat-button-toggle-appearance-standard {
  color: var(--%NS%mat-button-toggle-text-color, var(--%NS%mat-sys-on-surface));
  background-color: var(--%NS%mat-button-toggle-background-color, transparent);
  font-family: var(--%NS%mat-button-toggle-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-toggle-label-text-size, var(--%NS%mat-sys-label-large-size));
  line-height: var(--%NS%mat-button-toggle-label-text-line-height, var(--%NS%mat-sys-label-large-line-height));
  font-weight: var(--%NS%mat-button-toggle-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  letter-spacing: var(--%NS%mat-button-toggle-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
}
.mat-button-toggle-group-appearance-standard .mat-button-toggle-appearance-standard + .mat-button-toggle-appearance-standard {
  border-left: solid 1px var(--%NS%mat-button-toggle-divider-color, var(--%NS%mat-sys-outline));
}
[dir=rtl] .mat-button-toggle-group-appearance-standard .mat-button-toggle-appearance-standard + .mat-button-toggle-appearance-standard {
  border-left: none;
  border-right: solid 1px var(--%NS%mat-button-toggle-divider-color, var(--%NS%mat-sys-outline));
}
.mat-button-toggle-group-appearance-standard.mat-button-toggle-vertical .mat-button-toggle-appearance-standard + .mat-button-toggle-appearance-standard {
  border-left: none;
  border-right: none;
  border-top: solid 1px var(--%NS%mat-button-toggle-divider-color, var(--%NS%mat-sys-outline));
}
.mat-button-toggle-appearance-standard.mat-button-toggle-checked {
  color: var(--%NS%mat-button-toggle-selected-state-text-color, var(--%NS%mat-sys-on-secondary-container));
  background-color: var(--%NS%mat-button-toggle-selected-state-background-color, var(--%NS%mat-sys-secondary-container));
}
.mat-button-toggle-appearance-standard.mat-button-toggle-disabled {
  color: var(--%NS%mat-button-toggle-disabled-state-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-button-toggle-disabled-state-background-color, transparent);
}
.mat-button-toggle-appearance-standard.mat-button-toggle-disabled .mat-pseudo-checkbox {
  --%NS%mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color: var(--%NS%mat-button-toggle-disabled-selected-state-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-button-toggle-appearance-standard.mat-button-toggle-disabled.mat-button-toggle-checked {
  color: var(--%NS%mat-button-toggle-disabled-selected-state-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-button-toggle-disabled-selected-state-background-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-button-toggle-appearance-standard .mat-button-toggle-focus-overlay {
  background-color: var(--%NS%mat-button-toggle-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mat-button-toggle-appearance-standard:hover .mat-button-toggle-focus-overlay {
  opacity: var(--%NS%mat-button-toggle-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-button-toggle-appearance-standard.cdk-keyboard-focused .mat-button-toggle-focus-overlay {
  opacity: var(--%NS%mat-button-toggle-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
@media (hover: none) {
  .mat-button-toggle-appearance-standard:hover .mat-button-toggle-focus-overlay {
    display: none;
  }
}

.mat-button-toggle-label-content {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  padding: 0 16px;
  line-height: var(--%NS%mat-button-toggle-legacy-height);
  position: relative;
}
.mat-button-toggle-appearance-standard .mat-button-toggle-label-content {
  padding: 0 12px;
  line-height: var(--%NS%mat-button-toggle-height, 40px);
}

.mat-button-toggle-label-content > * {
  vertical-align: middle;
}

.mat-button-toggle-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
  pointer-events: none;
  opacity: 0;
  background-color: var(--%NS%mat-button-toggle-legacy-state-layer-color);
}

@media (forced-colors: active) {
  .mat-button-toggle-checked .mat-button-toggle-focus-overlay {
    border-bottom: solid 500px;
    opacity: 0.5;
    height: 0;
  }
  .mat-button-toggle-checked:hover .mat-button-toggle-focus-overlay {
    opacity: 0.6;
  }
  .mat-button-toggle-checked.mat-button-toggle-appearance-standard .mat-button-toggle-focus-overlay {
    border-bottom: solid 500px;
  }
}
.mat-button-toggle .mat-button-toggle-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}

.mat-button-toggle-button {
  border: 0;
  background: none;
  color: inherit;
  padding: 0;
  margin: 0;
  font: inherit;
  outline: none;
  width: 100%;
  cursor: pointer;
}
.mat-button-toggle-animations-enabled .mat-button-toggle-button {
  transition: padding 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-button-toggle-vertical .mat-button-toggle-button {
  transition: none;
}
.mat-button-toggle-disabled .mat-button-toggle-button {
  cursor: default;
}
.mat-button-toggle-button::-moz-focus-inner {
  border: 0;
}
.mat-button-toggle-checked .mat-button-toggle-button:has(.mat-button-toggle-checkbox-wrapper) {
  padding-left: 30px;
}
[dir=rtl] .mat-button-toggle-checked .mat-button-toggle-button:has(.mat-button-toggle-checkbox-wrapper) {
  padding-left: 0;
  padding-right: 30px;
}

.mat-button-toggle-standalone.mat-button-toggle-appearance-standard {
  --%NS%mat-focus-indicator-border-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
}

.mat-button-toggle-group-appearance-standard:not(.mat-button-toggle-vertical) .mat-button-toggle:last-of-type .mat-button-toggle-button::before {
  border-top-right-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
  border-bottom-right-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
}
.mat-button-toggle-group-appearance-standard:not(.mat-button-toggle-vertical) .mat-button-toggle:first-of-type .mat-button-toggle-button::before {
  border-top-left-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
  border-bottom-left-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
}

.mat-button-toggle-group-appearance-standard.mat-button-toggle-vertical .mat-button-toggle:last-of-type .mat-button-toggle-button::before {
  border-bottom-right-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
  border-bottom-left-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
}
.mat-button-toggle-group-appearance-standard.mat-button-toggle-vertical .mat-button-toggle:first-of-type .mat-button-toggle-button::before {
  border-top-right-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
  border-top-left-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
}
`],encapsulation:2})})()}return t})(),jy=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=G({imports:[Sn,dl,me]})}return t})();var Bp=(()=>{class t{get vertical(){return this._vertical}set vertical(e){this._vertical=kt(e)}_vertical=!1;get inset(){return this._inset}set inset(e){this._inset=kt(e)}_inset=!1;static \u0275fac=function(i){return new(i||t)};static \u0275cmp=B({type:t,selectors:[["mat-divider"]],hostAttrs:["role","separator",1,"mat-divider"],hostVars:7,hostBindings:function(i,a){i&2&&(K("aria-orientation",a.vertical?"vertical":"horizontal"),S("mat-divider-vertical",a.vertical)("mat-divider-horizontal",!a.vertical)("mat-divider-inset",a.inset))},inputs:{vertical:"vertical",inset:"inset"},decls:0,vars:0,template:function(i,a){},styles:[`.mat-divider {
  display: block;
  margin: 0;
  border-top-style: solid;
  border-top-color: var(--%NS%mat-divider-color, var(--%NS%mat-sys-outline-variant));
  border-top-width: var(--%NS%mat-divider-width, 1px);
}
.mat-divider.mat-divider-vertical {
  border-top: 0;
  border-right-style: solid;
  border-right-color: var(--%NS%mat-divider-color, var(--%NS%mat-sys-outline-variant));
  border-right-width: var(--%NS%mat-divider-width, 1px);
}
.mat-divider.mat-divider-inset {
  margin-left: 80px;
}
[dir=rtl] .mat-divider.mat-divider-inset {
  margin-left: auto;
  margin-right: 80px;
}
`],encapsulation:2})}return t})(),ml=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=G({imports:[me]})}return t})();var zy=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=G({imports:[me]})}return t})();var lN=new y("ListOption"),jp=(()=>{class t{_elementRef=m(L);static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["","matListItemTitle",""]],hostAttrs:[1,"mat-mdc-list-item-title","mdc-list-item__primary-text"]})}return t})(),zp=(()=>{class t{_elementRef=m(L);static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["","matListItemLine",""]],hostAttrs:[1,"mat-mdc-list-item-line","mdc-list-item__secondary-text"]})}return t})(),Hp=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["","matListItemMeta",""]],hostAttrs:[1,"mat-mdc-list-item-meta","mdc-list-item__end"]})}return t})(),Hy=(()=>{class t{_listOption=m(lN,{optional:!0});_isAlignedAtStart(){return!this._listOption||this._listOption?._getTogglePosition()==="after"}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,hostVars:4,hostBindings:function(i,a){i&2&&S("mdc-list-item__start",a._isAlignedAtStart())("mdc-list-item__end",!a._isAlignedAtStart())}})}return t})(),dN=(()=>{class t extends Hy{static \u0275fac=(()=>{let e;return function(a){return(e||(e=Fe(t)))(a||t)}})();static \u0275dir=D({type:t,selectors:[["","matListItemAvatar",""]],hostAttrs:[1,"mat-mdc-list-item-avatar"],features:[le]})}return t})(),mN=(()=>{class t extends Hy{static \u0275fac=(()=>{let e;return function(a){return(e||(e=Fe(t)))(a||t)}})();static \u0275dir=D({type:t,selectors:[["","matListItemIcon",""]],hostAttrs:[1,"mat-mdc-list-item-icon"],features:[le]})}return t})(),uN=new y("MAT_LIST_CONFIG"),so=(()=>{class t{_isNonInteractive=!0;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=kt(e)}_disableRipple=!1;get disabled(){return this._disabled()}set disabled(e){this._disabled.set(kt(e))}_disabled=ae(!1);_defaultOptions=m(uN,{optional:!0});static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,hostVars:1,hostBindings:function(i,a){i&2&&K("aria-disabled",a.disabled)},inputs:{disableRipple:"disableRipple",disabled:"disabled"}})}return t})(),pN=(()=>{class t{_elementRef=m(L);_ngZone=m(N);_listBase=m(so,{optional:!0});_platform=m(ye);_hostElement;_isButtonElement;_noopAnimations=Re();_avatars;_icons;set lines(e){this._explicitLines=Qi(e,null),this._updateItemLines(!1)}_explicitLines=null;get disableRipple(){return this.disabled||this._disableRipple||this._noopAnimations||!!this._listBase?.disableRipple}set disableRipple(e){this._disableRipple=kt(e)}_disableRipple=!1;get disabled(){return this._disabled()||!!this._listBase?.disabled}set disabled(e){this._disabled.set(kt(e))}_disabled=ae(!1);_subscriptions=new pe;_rippleRenderer=null;_hasUnscopedTextContent=!1;rippleConfig;get rippleDisabled(){return this.disableRipple||!!this.rippleConfig.disabled}constructor(){m(je).load(Ct);let e=m(mi,{optional:!0});this.rippleConfig=e||{},this._hostElement=this._elementRef.nativeElement,this._isButtonElement=this._hostElement.nodeName.toLowerCase()==="button",this._listBase&&!this._listBase._isNonInteractive&&this._initInteractiveListItem(),this._isButtonElement&&!this._hostElement.hasAttribute("type")&&this._hostElement.setAttribute("type","button")}ngAfterViewInit(){this._monitorProjectedLinesAndTitle(),this._updateItemLines(!0)}ngOnDestroy(){this._subscriptions.unsubscribe(),this._rippleRenderer!==null&&this._rippleRenderer._removeTriggerEvents()}_hasIconOrAvatar(){return!!(this._avatars.length||this._icons.length)}_initInteractiveListItem(){this._hostElement.classList.add("mat-mdc-list-item-interactive"),this._rippleRenderer=new ta(this,this._ngZone,this._hostElement,this._platform,m(X)),this._rippleRenderer.setupTriggerEvents(this._hostElement)}_monitorProjectedLinesAndTitle(){this._ngZone.runOutsideAngular(()=>{this._subscriptions.add(jt(this._lines.changes,this._titles.changes).subscribe(()=>this._updateItemLines(!1)))})}_updateItemLines(e){if(!this._lines||!this._titles||!this._unscopedContent)return;e&&this._checkDomForUnscopedTextContent();let i=this._explicitLines??this._inferLinesFromContent(),a=this._unscopedContent.nativeElement;if(this._hostElement.classList.toggle("mat-mdc-list-item-single-line",i<=1),this._hostElement.classList.toggle("mdc-list-item--with-one-line",i<=1),this._hostElement.classList.toggle("mdc-list-item--with-two-lines",i===2),this._hostElement.classList.toggle("mdc-list-item--with-three-lines",i===3),this._hasUnscopedTextContent){let r=this._titles.length===0&&i===1;a.classList.toggle("mdc-list-item__primary-text",r),a.classList.toggle("mdc-list-item__secondary-text",!r)}else a.classList.remove("mdc-list-item__primary-text"),a.classList.remove("mdc-list-item__secondary-text")}_inferLinesFromContent(){let e=this._titles.length+this._lines.length;return this._hasUnscopedTextContent&&(e+=1),e}_checkDomForUnscopedTextContent(){this._hasUnscopedTextContent=Array.from(this._unscopedContent.nativeElement.childNodes).filter(e=>e.nodeType!==e.COMMENT_NODE).some(e=>!!(e.textContent&&e.textContent.trim()))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,contentQueries:function(i,a,r){if(i&1&&Ye(r,dN,4)(r,mN,4),i&2){let o;M(o=T())&&(a._avatars=o),M(o=T())&&(a._icons=o)}},hostVars:4,hostBindings:function(i,a){i&2&&(K("aria-disabled",a.disabled)("disabled",a._isButtonElement&&a.disabled||null),S("mdc-list-item--disabled",a.disabled))},inputs:{lines:"lines",disableRipple:"disableRipple",disabled:"disabled"}})}return t})();var Uy=(()=>{class t extends so{static \u0275fac=(()=>{let e;return function(a){return(e||(e=Fe(t)))(a||t)}})();static \u0275cmp=(function(){let e=["*"];return B({type:t,selectors:[["mat-list"]],hostAttrs:[1,"mat-mdc-list","mat-mdc-list-base","mdc-list"],exportAs:["matList"],features:[_e([{provide:so,useExisting:t}]),le],ngContentSelectors:e,decls:1,vars:0,template:function(a,r){a&1&&(he(),V(0))},styles:[`.mdc-list {
  margin: 0;
  padding: 8px 0;
  list-style-type: none;
}
.mdc-list:focus {
  outline: none;
}

.mdc-list-item {
  display: flex;
  position: relative;
  justify-content: flex-start;
  overflow: hidden;
  padding: 0;
  align-items: stretch;
  cursor: pointer;
  padding-left: 16px;
  padding-right: 16px;
  background-color: var(--%NS%mat-list-list-item-container-color, transparent);
  border-radius: var(--%NS%mat-list-list-item-container-shape, var(--%NS%mat-sys-corner-none));
}
.mdc-list-item.mdc-list-item--selected {
  background-color: var(--%NS%mat-list-list-item-selected-container-color);
}
.mdc-list-item:focus {
  outline: 0;
}
.mdc-list-item.mdc-list-item--disabled {
  cursor: auto;
}
.mdc-list-item.mdc-list-item--with-one-line {
  height: var(--%NS%mat-list-list-item-one-line-container-height, 48px);
}
.mdc-list-item.mdc-list-item--with-one-line .mdc-list-item__start {
  align-self: center;
  margin-top: 0;
}
.mdc-list-item.mdc-list-item--with-one-line .mdc-list-item__end {
  align-self: center;
  margin-top: 0;
}
.mdc-list-item.mdc-list-item--with-two-lines {
  height: var(--%NS%mat-list-list-item-two-line-container-height, 64px);
}
.mdc-list-item.mdc-list-item--with-two-lines .mdc-list-item__start {
  align-self: flex-start;
  margin-top: 16px;
}
.mdc-list-item.mdc-list-item--with-two-lines .mdc-list-item__end {
  align-self: center;
  margin-top: 0;
}
.mdc-list-item.mdc-list-item--with-three-lines {
  height: var(--%NS%mat-list-list-item-three-line-container-height, 88px);
}
.mdc-list-item.mdc-list-item--with-three-lines .mdc-list-item__start {
  align-self: flex-start;
  margin-top: 16px;
}
.mdc-list-item.mdc-list-item--with-three-lines .mdc-list-item__end {
  align-self: flex-start;
  margin-top: 16px;
}
.mdc-list-item.mdc-list-item--%NS%selected::before, .mdc-list-item.mdc-list-item--%NS%selected:focus::before, .mdc-list-item:not(.mdc-list-item--selected):focus::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  content: "";
  pointer-events: none;
}

a.mdc-list-item {
  color: inherit;
  text-decoration: none;
}

.mdc-list-item__start {
  fill: currentColor;
  flex-shrink: 0;
  pointer-events: none;
}
.mdc-list-item--with-leading-icon .mdc-list-item__start {
  color: var(--%NS%mat-list-list-item-leading-icon-color, var(--%NS%mat-sys-on-surface-variant));
  width: var(--%NS%mat-list-list-item-leading-icon-size, 24px);
  height: var(--%NS%mat-list-list-item-leading-icon-size, 24px);
  margin-left: 16px;
  margin-right: 32px;
}
[dir=rtl] .mdc-list-item--with-leading-icon .mdc-list-item__start {
  margin-left: 32px;
  margin-right: 16px;
}
.mdc-list-item--%NS%with-leading-icon:hover .mdc-list-item__start {
  color: var(--%NS%mat-list-list-item-hover-leading-icon-color);
}
.mdc-list-item--with-leading-avatar .mdc-list-item__start {
  width: var(--%NS%mat-list-list-item-leading-avatar-size, 40px);
  height: var(--%NS%mat-list-list-item-leading-avatar-size, 40px);
  margin-left: 16px;
  margin-right: 16px;
  border-radius: 50%;
}
.mdc-list-item--with-leading-avatar .mdc-list-item__start, [dir=rtl] .mdc-list-item--with-leading-avatar .mdc-list-item__start {
  margin-left: 16px;
  margin-right: 16px;
  border-radius: 50%;
}

.mdc-list-item__end {
  flex-shrink: 0;
  pointer-events: none;
}
.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  font-family: var(--%NS%mat-list-list-item-trailing-supporting-text-font, var(--%NS%mat-sys-label-small-font));
  line-height: var(--%NS%mat-list-list-item-trailing-supporting-text-line-height, var(--%NS%mat-sys-label-small-line-height));
  font-size: var(--%NS%mat-list-list-item-trailing-supporting-text-size, var(--%NS%mat-sys-label-small-size));
  font-weight: var(--%NS%mat-list-list-item-trailing-supporting-text-weight, var(--%NS%mat-sys-label-small-weight));
  letter-spacing: var(--%NS%mat-list-list-item-trailing-supporting-text-tracking, var(--%NS%mat-sys-label-small-tracking));
}
.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  color: var(--%NS%mat-list-list-item-trailing-icon-color, var(--%NS%mat-sys-on-surface-variant));
  width: var(--%NS%mat-list-list-item-trailing-icon-size, 24px);
  height: var(--%NS%mat-list-list-item-trailing-icon-size, 24px);
}
.mdc-list-item--%NS%with-trailing-icon:hover .mdc-list-item__end {
  color: var(--%NS%mat-list-list-item-hover-trailing-icon-color);
}
.mdc-list-item.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  color: var(--%NS%mat-list-list-item-trailing-supporting-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-list-item--selected.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  color: var(--%NS%mat-list-list-item-selected-trailing-icon-color, var(--%NS%mat-sys-primary));
}

.mdc-list-item__content {
  text-overflow: ellipsis;
  white-space: nowrap;
  overflow: hidden;
  align-self: center;
  flex: 1;
  pointer-events: none;
}
.mdc-list-item--with-two-lines .mdc-list-item__content, .mdc-list-item--with-three-lines .mdc-list-item__content {
  align-self: stretch;
}

.mdc-list-item__primary-text {
  text-overflow: ellipsis;
  white-space: nowrap;
  overflow: hidden;
  color: var(--%NS%mat-list-list-item-label-text-color, var(--%NS%mat-sys-on-surface));
  font-family: var(--%NS%mat-list-list-item-label-text-font, var(--%NS%mat-sys-body-large-font));
  line-height: var(--%NS%mat-list-list-item-label-text-line-height, var(--%NS%mat-sys-body-large-line-height));
  font-size: var(--%NS%mat-list-list-item-label-text-size, var(--%NS%mat-sys-body-large-size));
  font-weight: var(--%NS%mat-list-list-item-label-text-weight, var(--%NS%mat-sys-body-large-weight));
  letter-spacing: var(--%NS%mat-list-list-item-label-text-tracking, var(--%NS%mat-sys-body-large-tracking));
}
.mdc-list-item:hover .mdc-list-item__primary-text {
  color: var(--%NS%mat-list-list-item-hover-label-text-color, var(--%NS%mat-sys-on-surface));
}
.mdc-list-item:focus .mdc-list-item__primary-text {
  color: var(--%NS%mat-list-list-item-focus-label-text-color, var(--%NS%mat-sys-on-surface));
}
.mdc-list-item--with-two-lines .mdc-list-item__primary-text, .mdc-list-item--with-three-lines .mdc-list-item__primary-text {
  display: block;
  margin-top: 0;
  line-height: normal;
  margin-bottom: -20px;
}
.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before, .mdc-list-item--with-three-lines .mdc-list-item__primary-text::before {
  display: inline-block;
  width: 0;
  height: 28px;
  content: "";
  vertical-align: 0;
}
.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after, .mdc-list-item--with-three-lines .mdc-list-item__primary-text::after {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: -20px;
}

.mdc-list-item__secondary-text {
  text-overflow: ellipsis;
  white-space: nowrap;
  overflow: hidden;
  display: block;
  margin-top: 0;
  color: var(--%NS%mat-list-list-item-supporting-text-color, var(--%NS%mat-sys-on-surface-variant));
  font-family: var(--%NS%mat-list-list-item-supporting-text-font, var(--%NS%mat-sys-body-medium-font));
  line-height: var(--%NS%mat-list-list-item-supporting-text-line-height, var(--%NS%mat-sys-body-medium-line-height));
  font-size: var(--%NS%mat-list-list-item-supporting-text-size, var(--%NS%mat-sys-body-medium-size));
  font-weight: var(--%NS%mat-list-list-item-supporting-text-weight, var(--%NS%mat-sys-body-medium-weight));
  letter-spacing: var(--%NS%mat-list-list-item-supporting-text-tracking, var(--%NS%mat-sys-body-medium-tracking));
}
.mdc-list-item__secondary-text::before {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: 0;
}
.mdc-list-item--with-three-lines .mdc-list-item__secondary-text {
  white-space: normal;
  line-height: 20px;
}
.mdc-list-item--with-overline .mdc-list-item__secondary-text {
  white-space: nowrap;
  line-height: auto;
}

.mdc-list-item--with-leading-radio.mdc-list-item,
.mdc-list-item--with-leading-checkbox.mdc-list-item,
.mdc-list-item--with-leading-icon.mdc-list-item,
.mdc-list-item--with-leading-avatar.mdc-list-item {
  padding-left: 0;
  padding-right: 16px;
}
[dir=rtl] .mdc-list-item--with-leading-radio.mdc-list-item,
[dir=rtl] .mdc-list-item--with-leading-checkbox.mdc-list-item,
[dir=rtl] .mdc-list-item--with-leading-icon.mdc-list-item,
[dir=rtl] .mdc-list-item--with-leading-avatar.mdc-list-item {
  padding-left: 16px;
  padding-right: 0;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__primary-text,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__primary-text,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines .mdc-list-item__primary-text,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text {
  display: block;
  margin-top: 0;
  line-height: normal;
  margin-bottom: -20px;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before {
  display: inline-block;
  width: 0;
  height: 32px;
  content: "";
  vertical-align: 0;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: -20px;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  display: block;
  margin-top: 0;
  line-height: normal;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before {
  display: inline-block;
  width: 0;
  height: 32px;
  content: "";
  vertical-align: 0;
}

.mdc-list-item--with-trailing-icon.mdc-list-item, [dir=rtl] .mdc-list-item--with-trailing-icon.mdc-list-item {
  padding-left: 0;
  padding-right: 0;
}
.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  margin-left: 16px;
  margin-right: 16px;
}

.mdc-list-item--with-trailing-meta.mdc-list-item {
  padding-left: 16px;
  padding-right: 0;
}
[dir=rtl] .mdc-list-item--with-trailing-meta.mdc-list-item {
  padding-left: 0;
  padding-right: 16px;
}
.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  -webkit-user-select: none;
  user-select: none;
  margin-left: 28px;
  margin-right: 16px;
}
[dir=rtl] .mdc-list-item--with-trailing-meta .mdc-list-item__end {
  margin-left: 16px;
  margin-right: 28px;
}
.mdc-list-item--with-trailing-meta.mdc-list-item--with-three-lines .mdc-list-item__end, .mdc-list-item--with-trailing-meta.mdc-list-item--with-two-lines .mdc-list-item__end {
  display: block;
  line-height: normal;
  align-self: flex-start;
  margin-top: 0;
}
.mdc-list-item--with-trailing-meta.mdc-list-item--with-three-lines .mdc-list-item__end::before, .mdc-list-item--with-trailing-meta.mdc-list-item--with-two-lines .mdc-list-item__end::before {
  display: inline-block;
  width: 0;
  height: 28px;
  content: "";
  vertical-align: 0;
}

.mdc-list-item--with-leading-radio .mdc-list-item__start,
.mdc-list-item--with-leading-checkbox .mdc-list-item__start {
  margin-left: 8px;
  margin-right: 24px;
}
[dir=rtl] .mdc-list-item--with-leading-radio .mdc-list-item__start,
[dir=rtl] .mdc-list-item--with-leading-checkbox .mdc-list-item__start {
  margin-left: 24px;
  margin-right: 8px;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__start,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__start {
  align-self: flex-start;
  margin-top: 8px;
}

.mdc-list-item--with-trailing-radio.mdc-list-item,
.mdc-list-item--with-trailing-checkbox.mdc-list-item {
  padding-left: 16px;
  padding-right: 0;
}
[dir=rtl] .mdc-list-item--with-trailing-radio.mdc-list-item,
[dir=rtl] .mdc-list-item--with-trailing-checkbox.mdc-list-item {
  padding-left: 0;
  padding-right: 16px;
}
.mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-icon, .mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-avatar,
.mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-icon,
.mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-avatar {
  padding-left: 0;
}
[dir=rtl] .mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-icon, [dir=rtl] .mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-avatar,
[dir=rtl] .mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-icon,
[dir=rtl] .mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-avatar {
  padding-right: 0;
}
.mdc-list-item--with-trailing-radio .mdc-list-item__end,
.mdc-list-item--with-trailing-checkbox .mdc-list-item__end {
  margin-left: 24px;
  margin-right: 8px;
}
[dir=rtl] .mdc-list-item--with-trailing-radio .mdc-list-item__end,
[dir=rtl] .mdc-list-item--with-trailing-checkbox .mdc-list-item__end {
  margin-left: 8px;
  margin-right: 24px;
}
.mdc-list-item--with-trailing-radio.mdc-list-item--with-three-lines .mdc-list-item__end,
.mdc-list-item--with-trailing-checkbox.mdc-list-item--with-three-lines .mdc-list-item__end {
  align-self: flex-start;
  margin-top: 8px;
}

.mdc-list-group__subheader {
  margin: 0.75rem 16px;
}

.mdc-list-item--disabled .mdc-list-item__start,
.mdc-list-item--disabled .mdc-list-item__content,
.mdc-list-item--disabled .mdc-list-item__end {
  opacity: 1;
}
.mdc-list-item--disabled .mdc-list-item__primary-text,
.mdc-list-item--disabled .mdc-list-item__secondary-text {
  opacity: var(--%NS%mat-list-list-item-disabled-label-text-opacity, 0.3);
}
.mdc-list-item--disabled.mdc-list-item--with-leading-icon .mdc-list-item__start {
  color: var(--%NS%mat-list-list-item-disabled-leading-icon-color, var(--%NS%mat-sys-on-surface));
  opacity: var(--%NS%mat-list-list-item-disabled-leading-icon-opacity, 0.38);
}
.mdc-list-item--disabled.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  color: var(--%NS%mat-list-list-item-disabled-trailing-icon-color, var(--%NS%mat-sys-on-surface));
  opacity: var(--%NS%mat-list-list-item-disabled-trailing-icon-opacity, 0.38);
}

.mat-mdc-list-item.mat-mdc-list-item-both-leading-and-trailing, [dir=rtl] .mat-mdc-list-item.mat-mdc-list-item-both-leading-and-trailing {
  padding-left: 0;
  padding-right: 0;
}

.mdc-list-item.mdc-list-item--disabled .mdc-list-item__primary-text {
  color: var(--%NS%mat-list-list-item-disabled-label-text-color, var(--%NS%mat-sys-on-surface));
}

.mdc-list-item:hover::before {
  background-color: var(--%NS%mat-list-list-item-hover-state-layer-color, var(--%NS%mat-sys-on-surface));
  opacity: var(--%NS%mat-list-list-item-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}

.mdc-list-item.mdc-list-item--%NS%disabled::before {
  background-color: var(--%NS%mat-list-list-item-disabled-state-layer-color, var(--%NS%mat-sys-on-surface));
  opacity: var(--%NS%mat-list-list-item-disabled-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}

.mdc-list-item:focus::before {
  background-color: var(--%NS%mat-list-list-item-focus-state-layer-color, var(--%NS%mat-sys-on-surface));
  opacity: var(--%NS%mat-list-list-item-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}

.mdc-list-item--disabled .mdc-radio,
.mdc-list-item--disabled .mdc-checkbox {
  opacity: var(--%NS%mat-list-list-item-disabled-label-text-opacity, 0.3);
}

.mdc-list-item--with-leading-avatar .mat-mdc-list-item-avatar {
  border-radius: var(--%NS%mat-list-list-item-leading-avatar-shape, var(--%NS%mat-sys-corner-full));
  background-color: var(--%NS%mat-list-list-item-leading-avatar-color, var(--%NS%mat-sys-primary-container));
}

.mat-mdc-list-item-icon {
  font-size: var(--%NS%mat-list-list-item-leading-icon-size, 24px);
}

@media (forced-colors: active) {
  a.mdc-list-item--%NS%activated::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  a.mdc-list-item--activated [dir=rtl]::after {
    right: auto;
    left: 16px;
  }
}

.mat-mdc-list-base {
  display: block;
}
.mat-mdc-list-base .mdc-list-item__start,
.mat-mdc-list-base .mdc-list-item__end,
.mat-mdc-list-base .mdc-list-item__content {
  pointer-events: auto;
}

.mat-mdc-list-item,
.mat-mdc-list-option {
  width: 100%;
  box-sizing: border-box;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-list-item:not(.mat-mdc-list-item-interactive),
.mat-mdc-list-option:not(.mat-mdc-list-item-interactive) {
  cursor: default;
}
.mat-mdc-list-item .mat-divider-inset,
.mat-mdc-list-option .mat-divider-inset {
  position: absolute;
  left: 0;
  right: 0;
  bottom: 0;
}
.mat-mdc-list-item .mat-mdc-list-item-avatar ~ .mat-divider-inset,
.mat-mdc-list-option .mat-mdc-list-item-avatar ~ .mat-divider-inset {
  margin-left: 72px;
}
[dir=rtl] .mat-mdc-list-item .mat-mdc-list-item-avatar ~ .mat-divider-inset,
[dir=rtl] .mat-mdc-list-option .mat-mdc-list-item-avatar ~ .mat-divider-inset {
  margin-right: 72px;
}

.mat-mdc-list-item-interactive::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  content: "";
  opacity: 0;
  pointer-events: none;
  border-radius: inherit;
}

.mat-mdc-list-item > .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
.mat-mdc-list-item:focus-visible > .mat-focus-indicator::before {
  content: "";
}

.mat-mdc-list-item.mdc-list-item--with-three-lines .mat-mdc-list-item-line.mdc-list-item__secondary-text {
  white-space: nowrap;
  line-height: normal;
}
.mat-mdc-list-item.mdc-list-item--with-three-lines .mat-mdc-list-item-unscoped-content.mdc-list-item__secondary-text {
  display: -webkit-box;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
}

mat-action-list button {
  background: none;
  color: inherit;
  border: none;
  font: inherit;
  outline: inherit;
  -webkit-tap-highlight-color: transparent;
  text-align: start;
}
mat-action-list button::-moz-focus-inner {
  border: 0;
}

.mdc-list-item--with-leading-icon .mdc-list-item__start {
  margin-inline-start: var(--%NS%mat-list-list-item-leading-icon-start-space, 16px);
  margin-inline-end: var(--%NS%mat-list-list-item-leading-icon-end-space, 16px);
}

.mat-mdc-nav-list .mat-mdc-list-item {
  border-radius: var(--%NS%mat-list-active-indicator-shape, var(--%NS%mat-sys-corner-full));
  --%NS%mat-focus-indicator-border-radius: var(--%NS%mat-list-active-indicator-shape, var(--%NS%mat-sys-corner-full));
}
.mat-mdc-nav-list .mat-mdc-list-item.mdc-list-item--activated {
  background-color: var(--%NS%mat-list-active-indicator-color, var(--%NS%mat-sys-secondary-container));
}
`],encapsulation:2})})()}return t})(),$y=(()=>{class t extends pN{_lines;_titles;_meta;_unscopedContent;_itemText;get activated(){return this._activated}set activated(e){this._activated=kt(e)}_activated=!1;_getAriaCurrent(){return this._hostElement.nodeName==="A"&&this._activated?"page":null}_hasBothLeadingAndTrailing(){return this._meta.length!==0&&(this._avatars.length!==0||this._icons.length!==0)}static \u0275fac=(()=>{let e;return function(a){return(e||(e=Fe(t)))(a||t)}})();static \u0275cmp=(function(){let e=["unscopedContent"],i=["text"],a=[[["","matListItemAvatar",""],["","matListItemIcon",""]],[["","matListItemTitle",""]],[["","matListItemLine",""]],"*",[["","matListItemMeta",""]],[["mat-divider"]]];return B({type:t,selectors:[["mat-list-item"],["a","mat-list-item",""],["button","mat-list-item",""]],contentQueries:function(s,c,l){if(s&1&&Ye(l,zp,5)(l,jp,5)(l,Hp,5),s&2){let d;M(d=T())&&(c._lines=d),M(d=T())&&(c._titles=d),M(d=T())&&(c._meta=d)}},viewQuery:function(s,c){if(s&1&&Be(e,5)(i,5),s&2){let l;M(l=T())&&(c._unscopedContent=l.first),M(l=T())&&(c._itemText=l.first)}},hostAttrs:[1,"mat-mdc-list-item","mdc-list-item"],hostVars:13,hostBindings:function(s,c){s&2&&(K("aria-current",c._getAriaCurrent()),S("mdc-list-item--activated",c.activated)("mdc-list-item--with-leading-avatar",c._avatars.length!==0)("mdc-list-item--with-leading-icon",c._icons.length!==0)("mdc-list-item--with-trailing-meta",c._meta.length!==0)("mat-mdc-list-item-both-leading-and-trailing",c._hasBothLeadingAndTrailing())("_mat-animation-noopable",c._noopAnimations))},inputs:{activated:"activated"},exportAs:["matListItem"],features:[le],ngContentSelectors:["[matListItemAvatar],[matListItemIcon]","[matListItemTitle]","[matListItemLine]","*","[matListItemMeta]","mat-divider"],decls:10,vars:0,consts:[["unscopedContent",""],[1,"mdc-list-item__content"],[1,"mat-mdc-list-item-unscoped-content",3,"cdkObserveContent"],[1,"mat-focus-indicator"]],template:function(s,c){s&1&&(he(a),V(0),p(1,"span",1),V(2,1),V(3,2),p(4,"span",2,0),C("cdkObserveContent",function(){return c._updateItemLines(!0)}),V(6,3),h()(),V(7,4),V(8,5),$(9,"div",3))},dependencies:[Gc],encapsulation:2})})()}return t})();var Gy=(()=>{class t extends so{_isNonInteractive=!1;static \u0275fac=(()=>{let e;return function(a){return(e||(e=Fe(t)))(a||t)}})();static \u0275cmp=(function(){let e=["*"];return B({type:t,selectors:[["mat-nav-list"]],hostAttrs:["role","navigation",1,"mat-mdc-nav-list","mat-mdc-list-base","mdc-list"],exportAs:["matNavList"],features:[_e([{provide:so,useExisting:t}]),le],ngContentSelectors:e,decls:1,vars:0,template:function(a,r){a&1&&(he(),V(0))},styles:[`.mdc-list {
  margin: 0;
  padding: 8px 0;
  list-style-type: none;
}
.mdc-list:focus {
  outline: none;
}

.mdc-list-item {
  display: flex;
  position: relative;
  justify-content: flex-start;
  overflow: hidden;
  padding: 0;
  align-items: stretch;
  cursor: pointer;
  padding-left: 16px;
  padding-right: 16px;
  background-color: var(--%NS%mat-list-list-item-container-color, transparent);
  border-radius: var(--%NS%mat-list-list-item-container-shape, var(--%NS%mat-sys-corner-none));
}
.mdc-list-item.mdc-list-item--selected {
  background-color: var(--%NS%mat-list-list-item-selected-container-color);
}
.mdc-list-item:focus {
  outline: 0;
}
.mdc-list-item.mdc-list-item--disabled {
  cursor: auto;
}
.mdc-list-item.mdc-list-item--with-one-line {
  height: var(--%NS%mat-list-list-item-one-line-container-height, 48px);
}
.mdc-list-item.mdc-list-item--with-one-line .mdc-list-item__start {
  align-self: center;
  margin-top: 0;
}
.mdc-list-item.mdc-list-item--with-one-line .mdc-list-item__end {
  align-self: center;
  margin-top: 0;
}
.mdc-list-item.mdc-list-item--with-two-lines {
  height: var(--%NS%mat-list-list-item-two-line-container-height, 64px);
}
.mdc-list-item.mdc-list-item--with-two-lines .mdc-list-item__start {
  align-self: flex-start;
  margin-top: 16px;
}
.mdc-list-item.mdc-list-item--with-two-lines .mdc-list-item__end {
  align-self: center;
  margin-top: 0;
}
.mdc-list-item.mdc-list-item--with-three-lines {
  height: var(--%NS%mat-list-list-item-three-line-container-height, 88px);
}
.mdc-list-item.mdc-list-item--with-three-lines .mdc-list-item__start {
  align-self: flex-start;
  margin-top: 16px;
}
.mdc-list-item.mdc-list-item--with-three-lines .mdc-list-item__end {
  align-self: flex-start;
  margin-top: 16px;
}
.mdc-list-item.mdc-list-item--%NS%selected::before, .mdc-list-item.mdc-list-item--%NS%selected:focus::before, .mdc-list-item:not(.mdc-list-item--selected):focus::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  content: "";
  pointer-events: none;
}

a.mdc-list-item {
  color: inherit;
  text-decoration: none;
}

.mdc-list-item__start {
  fill: currentColor;
  flex-shrink: 0;
  pointer-events: none;
}
.mdc-list-item--with-leading-icon .mdc-list-item__start {
  color: var(--%NS%mat-list-list-item-leading-icon-color, var(--%NS%mat-sys-on-surface-variant));
  width: var(--%NS%mat-list-list-item-leading-icon-size, 24px);
  height: var(--%NS%mat-list-list-item-leading-icon-size, 24px);
  margin-left: 16px;
  margin-right: 32px;
}
[dir=rtl] .mdc-list-item--with-leading-icon .mdc-list-item__start {
  margin-left: 32px;
  margin-right: 16px;
}
.mdc-list-item--%NS%with-leading-icon:hover .mdc-list-item__start {
  color: var(--%NS%mat-list-list-item-hover-leading-icon-color);
}
.mdc-list-item--with-leading-avatar .mdc-list-item__start {
  width: var(--%NS%mat-list-list-item-leading-avatar-size, 40px);
  height: var(--%NS%mat-list-list-item-leading-avatar-size, 40px);
  margin-left: 16px;
  margin-right: 16px;
  border-radius: 50%;
}
.mdc-list-item--with-leading-avatar .mdc-list-item__start, [dir=rtl] .mdc-list-item--with-leading-avatar .mdc-list-item__start {
  margin-left: 16px;
  margin-right: 16px;
  border-radius: 50%;
}

.mdc-list-item__end {
  flex-shrink: 0;
  pointer-events: none;
}
.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  font-family: var(--%NS%mat-list-list-item-trailing-supporting-text-font, var(--%NS%mat-sys-label-small-font));
  line-height: var(--%NS%mat-list-list-item-trailing-supporting-text-line-height, var(--%NS%mat-sys-label-small-line-height));
  font-size: var(--%NS%mat-list-list-item-trailing-supporting-text-size, var(--%NS%mat-sys-label-small-size));
  font-weight: var(--%NS%mat-list-list-item-trailing-supporting-text-weight, var(--%NS%mat-sys-label-small-weight));
  letter-spacing: var(--%NS%mat-list-list-item-trailing-supporting-text-tracking, var(--%NS%mat-sys-label-small-tracking));
}
.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  color: var(--%NS%mat-list-list-item-trailing-icon-color, var(--%NS%mat-sys-on-surface-variant));
  width: var(--%NS%mat-list-list-item-trailing-icon-size, 24px);
  height: var(--%NS%mat-list-list-item-trailing-icon-size, 24px);
}
.mdc-list-item--%NS%with-trailing-icon:hover .mdc-list-item__end {
  color: var(--%NS%mat-list-list-item-hover-trailing-icon-color);
}
.mdc-list-item.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  color: var(--%NS%mat-list-list-item-trailing-supporting-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-list-item--selected.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  color: var(--%NS%mat-list-list-item-selected-trailing-icon-color, var(--%NS%mat-sys-primary));
}

.mdc-list-item__content {
  text-overflow: ellipsis;
  white-space: nowrap;
  overflow: hidden;
  align-self: center;
  flex: 1;
  pointer-events: none;
}
.mdc-list-item--with-two-lines .mdc-list-item__content, .mdc-list-item--with-three-lines .mdc-list-item__content {
  align-self: stretch;
}

.mdc-list-item__primary-text {
  text-overflow: ellipsis;
  white-space: nowrap;
  overflow: hidden;
  color: var(--%NS%mat-list-list-item-label-text-color, var(--%NS%mat-sys-on-surface));
  font-family: var(--%NS%mat-list-list-item-label-text-font, var(--%NS%mat-sys-body-large-font));
  line-height: var(--%NS%mat-list-list-item-label-text-line-height, var(--%NS%mat-sys-body-large-line-height));
  font-size: var(--%NS%mat-list-list-item-label-text-size, var(--%NS%mat-sys-body-large-size));
  font-weight: var(--%NS%mat-list-list-item-label-text-weight, var(--%NS%mat-sys-body-large-weight));
  letter-spacing: var(--%NS%mat-list-list-item-label-text-tracking, var(--%NS%mat-sys-body-large-tracking));
}
.mdc-list-item:hover .mdc-list-item__primary-text {
  color: var(--%NS%mat-list-list-item-hover-label-text-color, var(--%NS%mat-sys-on-surface));
}
.mdc-list-item:focus .mdc-list-item__primary-text {
  color: var(--%NS%mat-list-list-item-focus-label-text-color, var(--%NS%mat-sys-on-surface));
}
.mdc-list-item--with-two-lines .mdc-list-item__primary-text, .mdc-list-item--with-three-lines .mdc-list-item__primary-text {
  display: block;
  margin-top: 0;
  line-height: normal;
  margin-bottom: -20px;
}
.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before, .mdc-list-item--with-three-lines .mdc-list-item__primary-text::before {
  display: inline-block;
  width: 0;
  height: 28px;
  content: "";
  vertical-align: 0;
}
.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after, .mdc-list-item--with-three-lines .mdc-list-item__primary-text::after {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: -20px;
}

.mdc-list-item__secondary-text {
  text-overflow: ellipsis;
  white-space: nowrap;
  overflow: hidden;
  display: block;
  margin-top: 0;
  color: var(--%NS%mat-list-list-item-supporting-text-color, var(--%NS%mat-sys-on-surface-variant));
  font-family: var(--%NS%mat-list-list-item-supporting-text-font, var(--%NS%mat-sys-body-medium-font));
  line-height: var(--%NS%mat-list-list-item-supporting-text-line-height, var(--%NS%mat-sys-body-medium-line-height));
  font-size: var(--%NS%mat-list-list-item-supporting-text-size, var(--%NS%mat-sys-body-medium-size));
  font-weight: var(--%NS%mat-list-list-item-supporting-text-weight, var(--%NS%mat-sys-body-medium-weight));
  letter-spacing: var(--%NS%mat-list-list-item-supporting-text-tracking, var(--%NS%mat-sys-body-medium-tracking));
}
.mdc-list-item__secondary-text::before {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: 0;
}
.mdc-list-item--with-three-lines .mdc-list-item__secondary-text {
  white-space: normal;
  line-height: 20px;
}
.mdc-list-item--with-overline .mdc-list-item__secondary-text {
  white-space: nowrap;
  line-height: auto;
}

.mdc-list-item--with-leading-radio.mdc-list-item,
.mdc-list-item--with-leading-checkbox.mdc-list-item,
.mdc-list-item--with-leading-icon.mdc-list-item,
.mdc-list-item--with-leading-avatar.mdc-list-item {
  padding-left: 0;
  padding-right: 16px;
}
[dir=rtl] .mdc-list-item--with-leading-radio.mdc-list-item,
[dir=rtl] .mdc-list-item--with-leading-checkbox.mdc-list-item,
[dir=rtl] .mdc-list-item--with-leading-icon.mdc-list-item,
[dir=rtl] .mdc-list-item--with-leading-avatar.mdc-list-item {
  padding-left: 16px;
  padding-right: 0;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__primary-text,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__primary-text,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines .mdc-list-item__primary-text,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text {
  display: block;
  margin-top: 0;
  line-height: normal;
  margin-bottom: -20px;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before {
  display: inline-block;
  width: 0;
  height: 32px;
  content: "";
  vertical-align: 0;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: -20px;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  display: block;
  margin-top: 0;
  line-height: normal;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before {
  display: inline-block;
  width: 0;
  height: 32px;
  content: "";
  vertical-align: 0;
}

.mdc-list-item--with-trailing-icon.mdc-list-item, [dir=rtl] .mdc-list-item--with-trailing-icon.mdc-list-item {
  padding-left: 0;
  padding-right: 0;
}
.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  margin-left: 16px;
  margin-right: 16px;
}

.mdc-list-item--with-trailing-meta.mdc-list-item {
  padding-left: 16px;
  padding-right: 0;
}
[dir=rtl] .mdc-list-item--with-trailing-meta.mdc-list-item {
  padding-left: 0;
  padding-right: 16px;
}
.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  -webkit-user-select: none;
  user-select: none;
  margin-left: 28px;
  margin-right: 16px;
}
[dir=rtl] .mdc-list-item--with-trailing-meta .mdc-list-item__end {
  margin-left: 16px;
  margin-right: 28px;
}
.mdc-list-item--with-trailing-meta.mdc-list-item--with-three-lines .mdc-list-item__end, .mdc-list-item--with-trailing-meta.mdc-list-item--with-two-lines .mdc-list-item__end {
  display: block;
  line-height: normal;
  align-self: flex-start;
  margin-top: 0;
}
.mdc-list-item--with-trailing-meta.mdc-list-item--with-three-lines .mdc-list-item__end::before, .mdc-list-item--with-trailing-meta.mdc-list-item--with-two-lines .mdc-list-item__end::before {
  display: inline-block;
  width: 0;
  height: 28px;
  content: "";
  vertical-align: 0;
}

.mdc-list-item--with-leading-radio .mdc-list-item__start,
.mdc-list-item--with-leading-checkbox .mdc-list-item__start {
  margin-left: 8px;
  margin-right: 24px;
}
[dir=rtl] .mdc-list-item--with-leading-radio .mdc-list-item__start,
[dir=rtl] .mdc-list-item--with-leading-checkbox .mdc-list-item__start {
  margin-left: 24px;
  margin-right: 8px;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__start,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__start {
  align-self: flex-start;
  margin-top: 8px;
}

.mdc-list-item--with-trailing-radio.mdc-list-item,
.mdc-list-item--with-trailing-checkbox.mdc-list-item {
  padding-left: 16px;
  padding-right: 0;
}
[dir=rtl] .mdc-list-item--with-trailing-radio.mdc-list-item,
[dir=rtl] .mdc-list-item--with-trailing-checkbox.mdc-list-item {
  padding-left: 0;
  padding-right: 16px;
}
.mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-icon, .mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-avatar,
.mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-icon,
.mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-avatar {
  padding-left: 0;
}
[dir=rtl] .mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-icon, [dir=rtl] .mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-avatar,
[dir=rtl] .mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-icon,
[dir=rtl] .mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-avatar {
  padding-right: 0;
}
.mdc-list-item--with-trailing-radio .mdc-list-item__end,
.mdc-list-item--with-trailing-checkbox .mdc-list-item__end {
  margin-left: 24px;
  margin-right: 8px;
}
[dir=rtl] .mdc-list-item--with-trailing-radio .mdc-list-item__end,
[dir=rtl] .mdc-list-item--with-trailing-checkbox .mdc-list-item__end {
  margin-left: 8px;
  margin-right: 24px;
}
.mdc-list-item--with-trailing-radio.mdc-list-item--with-three-lines .mdc-list-item__end,
.mdc-list-item--with-trailing-checkbox.mdc-list-item--with-three-lines .mdc-list-item__end {
  align-self: flex-start;
  margin-top: 8px;
}

.mdc-list-group__subheader {
  margin: 0.75rem 16px;
}

.mdc-list-item--disabled .mdc-list-item__start,
.mdc-list-item--disabled .mdc-list-item__content,
.mdc-list-item--disabled .mdc-list-item__end {
  opacity: 1;
}
.mdc-list-item--disabled .mdc-list-item__primary-text,
.mdc-list-item--disabled .mdc-list-item__secondary-text {
  opacity: var(--%NS%mat-list-list-item-disabled-label-text-opacity, 0.3);
}
.mdc-list-item--disabled.mdc-list-item--with-leading-icon .mdc-list-item__start {
  color: var(--%NS%mat-list-list-item-disabled-leading-icon-color, var(--%NS%mat-sys-on-surface));
  opacity: var(--%NS%mat-list-list-item-disabled-leading-icon-opacity, 0.38);
}
.mdc-list-item--disabled.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  color: var(--%NS%mat-list-list-item-disabled-trailing-icon-color, var(--%NS%mat-sys-on-surface));
  opacity: var(--%NS%mat-list-list-item-disabled-trailing-icon-opacity, 0.38);
}

.mat-mdc-list-item.mat-mdc-list-item-both-leading-and-trailing, [dir=rtl] .mat-mdc-list-item.mat-mdc-list-item-both-leading-and-trailing {
  padding-left: 0;
  padding-right: 0;
}

.mdc-list-item.mdc-list-item--disabled .mdc-list-item__primary-text {
  color: var(--%NS%mat-list-list-item-disabled-label-text-color, var(--%NS%mat-sys-on-surface));
}

.mdc-list-item:hover::before {
  background-color: var(--%NS%mat-list-list-item-hover-state-layer-color, var(--%NS%mat-sys-on-surface));
  opacity: var(--%NS%mat-list-list-item-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}

.mdc-list-item.mdc-list-item--%NS%disabled::before {
  background-color: var(--%NS%mat-list-list-item-disabled-state-layer-color, var(--%NS%mat-sys-on-surface));
  opacity: var(--%NS%mat-list-list-item-disabled-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}

.mdc-list-item:focus::before {
  background-color: var(--%NS%mat-list-list-item-focus-state-layer-color, var(--%NS%mat-sys-on-surface));
  opacity: var(--%NS%mat-list-list-item-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}

.mdc-list-item--disabled .mdc-radio,
.mdc-list-item--disabled .mdc-checkbox {
  opacity: var(--%NS%mat-list-list-item-disabled-label-text-opacity, 0.3);
}

.mdc-list-item--with-leading-avatar .mat-mdc-list-item-avatar {
  border-radius: var(--%NS%mat-list-list-item-leading-avatar-shape, var(--%NS%mat-sys-corner-full));
  background-color: var(--%NS%mat-list-list-item-leading-avatar-color, var(--%NS%mat-sys-primary-container));
}

.mat-mdc-list-item-icon {
  font-size: var(--%NS%mat-list-list-item-leading-icon-size, 24px);
}

@media (forced-colors: active) {
  a.mdc-list-item--%NS%activated::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  a.mdc-list-item--activated [dir=rtl]::after {
    right: auto;
    left: 16px;
  }
}

.mat-mdc-list-base {
  display: block;
}
.mat-mdc-list-base .mdc-list-item__start,
.mat-mdc-list-base .mdc-list-item__end,
.mat-mdc-list-base .mdc-list-item__content {
  pointer-events: auto;
}

.mat-mdc-list-item,
.mat-mdc-list-option {
  width: 100%;
  box-sizing: border-box;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-list-item:not(.mat-mdc-list-item-interactive),
.mat-mdc-list-option:not(.mat-mdc-list-item-interactive) {
  cursor: default;
}
.mat-mdc-list-item .mat-divider-inset,
.mat-mdc-list-option .mat-divider-inset {
  position: absolute;
  left: 0;
  right: 0;
  bottom: 0;
}
.mat-mdc-list-item .mat-mdc-list-item-avatar ~ .mat-divider-inset,
.mat-mdc-list-option .mat-mdc-list-item-avatar ~ .mat-divider-inset {
  margin-left: 72px;
}
[dir=rtl] .mat-mdc-list-item .mat-mdc-list-item-avatar ~ .mat-divider-inset,
[dir=rtl] .mat-mdc-list-option .mat-mdc-list-item-avatar ~ .mat-divider-inset {
  margin-right: 72px;
}

.mat-mdc-list-item-interactive::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  content: "";
  opacity: 0;
  pointer-events: none;
  border-radius: inherit;
}

.mat-mdc-list-item > .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
.mat-mdc-list-item:focus-visible > .mat-focus-indicator::before {
  content: "";
}

.mat-mdc-list-item.mdc-list-item--with-three-lines .mat-mdc-list-item-line.mdc-list-item__secondary-text {
  white-space: nowrap;
  line-height: normal;
}
.mat-mdc-list-item.mdc-list-item--with-three-lines .mat-mdc-list-item-unscoped-content.mdc-list-item__secondary-text {
  display: -webkit-box;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
}

mat-action-list button {
  background: none;
  color: inherit;
  border: none;
  font: inherit;
  outline: inherit;
  -webkit-tap-highlight-color: transparent;
  text-align: start;
}
mat-action-list button::-moz-focus-inner {
  border: 0;
}

.mdc-list-item--with-leading-icon .mdc-list-item__start {
  margin-inline-start: var(--%NS%mat-list-list-item-leading-icon-start-space, 16px);
  margin-inline-end: var(--%NS%mat-list-list-item-leading-icon-end-space, 16px);
}

.mat-mdc-nav-list .mat-mdc-list-item {
  border-radius: var(--%NS%mat-list-active-indicator-shape, var(--%NS%mat-sys-corner-full));
  --%NS%mat-focus-indicator-border-radius: var(--%NS%mat-list-active-indicator-shape, var(--%NS%mat-sys-corner-full));
}
.mat-mdc-nav-list .mat-mdc-list-item.mdc-list-item--activated {
  background-color: var(--%NS%mat-list-active-indicator-color, var(--%NS%mat-sys-secondary-container));
}
`],encapsulation:2})})()}return t})();var Wy=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=G({imports:[Wc,Sn,zy,me,ml]})}return t})();var fN=new y("MAT_CARD_CONFIG"),qy=(()=>{class t{appearance;constructor(){let e=m(fN,{optional:!0});this.appearance=e?.appearance||"raised"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){return B({type:t,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(a,r){a&2&&S("mat-mdc-card-outlined",r.appearance==="outlined")("mdc-card--outlined",r.appearance==="outlined")("mat-mdc-card-filled",r.appearance==="filled")("mdc-card--filled",r.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:["*"],decls:1,vars:0,template:function(a,r){a&1&&(he(),V(0))},styles:[`.mat-mdc-card {
  display: flex;
  flex-direction: column;
  box-sizing: border-box;
  position: relative;
  border-style: solid;
  border-width: 0;
  background-color: var(--%NS%mat-card-elevated-container-color, var(--%NS%mat-sys-surface-container-low));
  border-color: var(--%NS%mat-card-elevated-container-color, var(--%NS%mat-sys-surface-container-low));
  border-radius: var(--%NS%mat-card-elevated-container-shape, var(--%NS%mat-sys-corner-medium));
  box-shadow: var(--%NS%mat-card-elevated-container-elevation, var(--%NS%mat-sys-level1));
}
.mat-mdc-card::after {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  border: solid 1px transparent;
  content: "";
  display: block;
  pointer-events: none;
  box-sizing: border-box;
  border-radius: var(--%NS%mat-card-elevated-container-shape, var(--%NS%mat-sys-corner-medium));
}

.mat-mdc-card-outlined {
  background-color: var(--%NS%mat-card-outlined-container-color, var(--%NS%mat-sys-surface));
  border-radius: var(--%NS%mat-card-outlined-container-shape, var(--%NS%mat-sys-corner-medium));
  border-width: var(--%NS%mat-card-outlined-outline-width, 1px);
  border-color: var(--%NS%mat-card-outlined-outline-color, var(--%NS%mat-sys-outline-variant));
  box-shadow: var(--%NS%mat-card-outlined-container-elevation, var(--%NS%mat-sys-level0));
}
.mat-mdc-card-outlined::after {
  border: none;
}

.mat-mdc-card-filled {
  background-color: var(--%NS%mat-card-filled-container-color, var(--%NS%mat-sys-surface-container-highest));
  border-radius: var(--%NS%mat-card-filled-container-shape, var(--%NS%mat-sys-corner-medium));
  box-shadow: var(--%NS%mat-card-filled-container-elevation, var(--%NS%mat-sys-level0));
}

.mdc-card__media {
  position: relative;
  box-sizing: border-box;
  background-repeat: no-repeat;
  background-position: center;
  background-size: cover;
}
.mdc-card__media::before {
  display: block;
  content: "";
}
.mdc-card__media:first-child {
  border-top-left-radius: inherit;
  border-top-right-radius: inherit;
}
.mdc-card__media:last-child {
  border-bottom-left-radius: inherit;
  border-bottom-right-radius: inherit;
}

.mat-mdc-card-actions {
  display: flex;
  flex-direction: row;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  padding: 8px;
}

.mat-mdc-card-title {
  font-family: var(--%NS%mat-card-title-text-font, var(--%NS%mat-sys-title-large-font));
  line-height: var(--%NS%mat-card-title-text-line-height, var(--%NS%mat-sys-title-large-line-height));
  font-size: var(--%NS%mat-card-title-text-size, var(--%NS%mat-sys-title-large-size));
  letter-spacing: var(--%NS%mat-card-title-text-tracking, var(--%NS%mat-sys-title-large-tracking));
  font-weight: var(--%NS%mat-card-title-text-weight, var(--%NS%mat-sys-title-large-weight));
}

.mat-mdc-card-subtitle {
  color: var(--%NS%mat-card-subtitle-text-color, var(--%NS%mat-sys-on-surface));
  font-family: var(--%NS%mat-card-subtitle-text-font, var(--%NS%mat-sys-title-medium-font));
  line-height: var(--%NS%mat-card-subtitle-text-line-height, var(--%NS%mat-sys-title-medium-line-height));
  font-size: var(--%NS%mat-card-subtitle-text-size, var(--%NS%mat-sys-title-medium-size));
  letter-spacing: var(--%NS%mat-card-subtitle-text-tracking, var(--%NS%mat-sys-title-medium-tracking));
  font-weight: var(--%NS%mat-card-subtitle-text-weight, var(--%NS%mat-sys-title-medium-weight));
}

.mat-mdc-card-title,
.mat-mdc-card-subtitle {
  display: block;
  margin: 0;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle {
  padding: 16px 16px 0;
}

.mat-mdc-card-header {
  display: flex;
  padding: 16px 16px 0;
}

.mat-mdc-card-content {
  display: block;
  padding: 0 16px;
}
.mat-mdc-card-content:first-child {
  padding-top: 16px;
}
.mat-mdc-card-content:last-child {
  padding-bottom: 16px;
}

.mat-mdc-card-title-group {
  display: flex;
  justify-content: space-between;
  width: 100%;
}

.mat-mdc-card-avatar {
  height: 40px;
  width: 40px;
  border-radius: 50%;
  flex-shrink: 0;
  margin-bottom: 16px;
  object-fit: cover;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title {
  line-height: normal;
}

.mat-mdc-card-sm-image {
  width: 80px;
  height: 80px;
}

.mat-mdc-card-md-image {
  width: 112px;
  height: 112px;
}

.mat-mdc-card-lg-image {
  width: 152px;
  height: 152px;
}

.mat-mdc-card-xl-image {
  width: 240px;
  height: 240px;
}

.mat-mdc-card-subtitle ~ .mat-mdc-card-title,
.mat-mdc-card-title ~ .mat-mdc-card-subtitle,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-title-group .mat-mdc-card-title,
.mat-mdc-card-title-group .mat-mdc-card-subtitle {
  padding-top: 0;
}

.mat-mdc-card-content > :last-child:not(.mat-mdc-card-footer) {
  margin-bottom: 0;
}

.mat-mdc-card-actions-align-end {
  justify-content: flex-end;
}
`],encapsulation:2})})()}return t})();var Yy=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["mat-card-content"]],hostAttrs:[1,"mat-mdc-card-content"]})}return t})();var Zy=(()=>{class t{align="start";static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["mat-card-actions"]],hostAttrs:[1,"mat-mdc-card-actions","mdc-card__actions"],hostVars:2,hostBindings:function(i,a){i&2&&S("mat-mdc-card-actions-align-end",a.align==="end")},inputs:{align:"align"},exportAs:["matCardActions"]})}return t})();var Ky=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=G({imports:[me]})}return t})();var co=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=G({imports:[Wc,sl,me]})}return t})();var vN=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=B({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(i,a){},styles:[`textarea.cdk-textarea-autosize {
  resize: none;
}

textarea.cdk-textarea-autosize-measuring {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: auto !important;
  overflow: hidden !important;
}

textarea.cdk-textarea-autosize-measuring-firefox {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: 0 !important;
}

@keyframes cdk-text-field-autofill-start { /*!*/ }
@keyframes cdk-text-field-autofill-end { /*!*/ }
.cdk-text-field-autofill-monitored:-webkit-autofill {
  animation: cdk-text-field-autofill-start 0s 1ms;
}

.cdk-text-field-autofill-monitored:not(:-webkit-autofill) {
  animation: cdk-text-field-autofill-end 0s 1ms;
}
`],encapsulation:2})}return t})(),_N={passive:!0},Qy=(()=>{class t{_platform=m(ye);_ngZone=m(N);_renderer=m(Je).createRenderer(null,null);_styleLoader=m(je);_monitoredElements=new Map;monitor(e){if(!this._platform.isBrowser)return Dn;this._styleLoader.load(vN);let i=wt(e),a=this._monitoredElements.get(i);if(a)return a.subject;let r=new E,o="cdk-text-field-autofilled",s=l=>{l.animationName==="cdk-text-field-autofill-start"&&!i.classList.contains(o)?(i.classList.add(o),this._ngZone.run(()=>r.next({target:l.target,isAutofilled:!0}))):l.animationName==="cdk-text-field-autofill-end"&&i.classList.contains(o)&&(i.classList.remove(o),this._ngZone.run(()=>r.next({target:l.target,isAutofilled:!1})))},c=this._ngZone.runOutsideAngular(()=>(i.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(i,"animationstart",s,_N)));return this._monitoredElements.set(i,{subject:r,unlisten:c}),r}stopMonitoring(e){let i=wt(e),a=this._monitoredElements.get(i);a&&(a.unlisten(),a.subject.complete(),i.classList.remove("cdk-text-field-autofill-monitored"),i.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(i))}ngOnDestroy(){this._monitoredElements.forEach((e,i)=>this.stopMonitoring(i))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})}return t})();var Xy=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=G({})}return t})();var Jy=new y("MAT_INPUT_VALUE_ACCESSOR");var yN=["button","checkbox","file","hidden","image","radio","range","reset","submit"],xN=new y("MAT_INPUT_CONFIG"),ex=(()=>{class t{_elementRef=m(L);_platform=m(ye);ngControl=m($n,{optional:!0,self:!0});_autofillMonitor=m(Qy);_ngZone=m(N);_formField=m(Mp,{optional:!0});_renderer=m(Se);_uid=m(Pe).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=m(xN,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new E;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=kt(e),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(e){this._id=e||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(ja.required)??!1}set required(e){this._required=kt(e)}_required;get type(){return this._type}set type(e){this._type=e||"text",this._validateType(),!this._isTextarea&&_p().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(e){e!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(e):this._inputValueAccessor.value=e,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(e){this._readonly=kt(e)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(e=>_p().has(e));constructor(){let e=m(Bc,{optional:!0}),i=m(jc,{optional:!0}),a=m(il),r=m(Jy,{optional:!0,self:!0}),o=m(nl,{optional:!0,self:!0}),s=this._elementRef.nativeElement,c=s.nodeName.toLowerCase();r?Lt(r.value)?this._signalBasedValueAccessor=r:this._inputValueAccessor=r:this._inputValueAccessor=s,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(s,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new al(a,o||this.ngControl,i,e,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=c==="select",this._isTextarea=c==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=s.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&Jt(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(e=>{this.autofilled=e.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(e){this._elementRef.nativeElement.focus(e)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(e){if(e!==this.focused){if(!this._isNativeSelect&&e&&this.disabled&&this.disabledInteractive){let i=this._elementRef.nativeElement;i.type==="number"?(i.type="text",i.setSelectionRange(0,0),i.type="number"):i.setSelectionRange(0,0)}this.focused=e,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let e=this._elementRef.nativeElement.value;this._previousNativeValue!==e&&(this._previousNativeValue=e,this.stateChanges.next())}_dirtyCheckPlaceholder(){let e=this._getPlaceholder();if(e!==this._previousPlaceholder){let i=this._elementRef.nativeElement;this._previousPlaceholder=e,e?i.setAttribute("placeholder",e):i.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){yN.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let e=this._elementRef.nativeElement.validity;return e&&e.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let e=this._elementRef.nativeElement,i=e.options[0];return this.focused||e.multiple||!this.empty||!!(e.selectedIndex>-1&&i&&i.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let e=this._elementRef.nativeElement;return this._isNativeSelect&&(e.multiple||e.size>1)}_iOSKeyupListener=e=>{let i=e.target;!i.value&&i.selectionStart===0&&i.selectionEnd===0&&(i.setSelectionRange(1,1),i.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(i,a){i&1&&C("focus",function(){return a._focusChanged(!0)})("blur",function(){return a._focusChanged(!1)})("input",function(){return a._onInput()}),i&2&&(Gt("id",a.id)("disabled",a.disabled&&!a.disabledInteractive)("required",a.required),K("name",a.name||null)("readonly",a._getReadonlyAttribute())("aria-disabled",a.disabled&&a.disabledInteractive?"true":null)("aria-invalid",a.empty&&a.required?null:a.errorState)("aria-required",a.required)("id",a.id),S("mat-input-server",a._isServer)("mat-mdc-form-field-textarea-control",a._isInFormField&&a._isTextarea)("mat-mdc-form-field-input-control",a._isInFormField)("mat-mdc-input-disabled-interactive",a.disabledInteractive)("mdc-text-field__input",a._isInFormField)("mat-mdc-native-select-inline",a._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",I]},exportAs:["matInput"],features:[_e([{provide:Np,useExisting:t}]),rt]})}return t})(),tx=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=G({imports:[co,co,Xy,me]})}return t})();var ul=(()=>{class t{labelPosition="after";static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){return B({type:t,selectors:[["","mat-internal-form-field",""]],hostAttrs:[1,"mdc-form-field","mat-internal-form-field"],hostVars:2,hostBindings:function(a,r){a&2&&S("mdc-form-field--align-end",r.labelPosition==="before")},inputs:{labelPosition:"labelPosition"},ngContentSelectors:["*"],decls:1,vars:0,template:function(a,r){a&1&&(he(),V(0))},styles:[`.mat-internal-form-field {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: inline-flex;
  align-items: center;
  vertical-align: middle;
}
.mat-internal-form-field > label, .mat-internal-form-field > .mat-internal-form-field-label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
  order: 0;
}
[dir=rtl] .mat-internal-form-field > label, [dir=rtl] .mat-internal-form-field > .mat-internal-form-field-label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
}

.mdc-form-field--align-end > label, .mdc-form-field--align-end > .mat-internal-form-field-label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
  order: -1;
}
[dir=rtl] .mdc-form-field--align-end .mdc-form-field--align-end label, [dir=rtl] .mdc-form-field--align-end .mdc-form-field--align-end .mat-internal-form-field-label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
}
`],encapsulation:2})})()}return t})();var Up={color:"accent",clickAction:"check-indeterminate",disabledInteractive:!1},kN=new y("mat-checkbox-default-options",{providedIn:"root",factory:()=>Up}),vt=(function(t){return t[t.Init=0]="Init",t[t.Checked=1]="Checked",t[t.Unchecked=2]="Unchecked",t[t.Indeterminate=3]="Indeterminate",t})(vt||{}),$p=class{source;checked},Gp=(()=>{class t{_elementRef=m(L);_changeDetectorRef=m(Ie);_ngZone=m(N);_animationsDisabled=Re();_options=m(kN,{optional:!0});_injector=m(X);_signalFormField=null;focus(){this._inputElement.nativeElement.focus()}_createChangeEvent(e){let i=new $p;return i.source=this,i.checked=e,i}_getAnimationTargetElement(){return this._inputElement?.nativeElement}_animationClasses={uncheckedToChecked:"mdc-checkbox--anim-unchecked-checked",uncheckedToIndeterminate:"mdc-checkbox--anim-unchecked-indeterminate",checkedToUnchecked:"mdc-checkbox--anim-checked-unchecked",checkedToIndeterminate:"mdc-checkbox--anim-checked-indeterminate",indeterminateToChecked:"mdc-checkbox--anim-indeterminate-checked",indeterminateToUnchecked:"mdc-checkbox--anim-indeterminate-unchecked"};ariaLabel="";ariaLabelledby=null;ariaDescribedby;ariaExpanded;ariaControls;ariaOwns;_uniqueId;id;get inputId(){return`${this.id||this._uniqueId}-input`}required=!1;labelPosition="after";name=null;change=new z;indeterminateChange=new z;value;disableRipple=!1;_inputElement;tabIndex;color;disabledInteractive;_onTouched=()=>{};_currentAnimationClass="";_currentCheckState=vt.Init;_controlValueAccessorChangeFn=()=>{};_validatorChangeFn=()=>{};constructor(){m(je).load(Ct);let e=m(new Un("tabindex"),{optional:!0});this._options=this._options||Up,this.color=this._options.color||Up.color,this.tabIndex=e==null?0:parseInt(e)||0,this.id=this._uniqueId=m(Pe).getId("mat-mdc-checkbox-"),this.disabledInteractive=this._options?.disabledInteractive??!1}ngOnChanges(e){e.required&&this._validatorChangeFn()}ngOnInit(){this._signalFormField=this._injector.get(nl,void 0,{optional:!0,self:!0})}ngAfterViewInit(){this._syncIndeterminate(this.indeterminate)}get checked(){return this._checked}set checked(e){e!=this.checked&&(this._checked=e,this._changeDetectorRef.markForCheck())}_checked=!1;get disabled(){return this._disabled}set disabled(e){e!==this.disabled&&(this._disabled=e,this._changeDetectorRef.markForCheck())}_disabled=!1;get indeterminate(){return this._indeterminate()}set indeterminate(e){let i=e!=this._indeterminate();this._indeterminate.set(e),i&&(e?this._transitionCheckState(vt.Indeterminate):this._transitionCheckState(this.checked?vt.Checked:vt.Unchecked),this.indeterminateChange.emit(e)),this._syncIndeterminate(e)}_indeterminate=ae(!1);_isRippleDisabled(){return this.disableRipple||this.disabled}_onLabelTextChange(){this._changeDetectorRef.detectChanges()}writeValue(e){this.checked=!!e}registerOnChange(e){this._controlValueAccessorChangeFn=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e}validate(e){return this.required&&e.value!==!0&&!this._signalFormField?{required:!0}:null}registerOnValidatorChange(e){this._validatorChangeFn=e}_transitionCheckState(e){let i=this._currentCheckState,a=this._getAnimationTargetElement();if(!(i===e||!a)&&(this._currentAnimationClass&&a.classList.remove(this._currentAnimationClass),this._currentAnimationClass=this._getAnimationClassForCheckStateTransition(i,e),this._currentCheckState=e,this._currentAnimationClass.length>0)){a.classList.add(this._currentAnimationClass);let r=this._currentAnimationClass;this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{a.classList.remove(r)},1e3)})}}_emitChangeEvent(){this._controlValueAccessorChangeFn(this.checked),this.change.emit(this._createChangeEvent(this.checked)),this._inputElement&&(this._inputElement.nativeElement.checked=this.checked)}toggle(){this.checked=!this.checked,this._controlValueAccessorChangeFn(this.checked)}_handleInputClick(){let e=this._options?.clickAction;!this.disabled&&e!=="noop"?(this.indeterminate&&e!=="check"&&Promise.resolve().then(()=>{this._indeterminate.set(!1),this.indeterminateChange.emit(!1)}),this._checked=!this._checked,this._transitionCheckState(this._checked?vt.Checked:vt.Unchecked),this._emitChangeEvent()):(this.disabled&&this.disabledInteractive||!this.disabled&&e==="noop")&&(this._inputElement.nativeElement.checked=this.checked,this._inputElement.nativeElement.indeterminate=this.indeterminate)}_onInteractionEvent(e){e.stopPropagation()}_onBlur(){Promise.resolve().then(()=>{this._onTouched(),this._changeDetectorRef.markForCheck()})}_getAnimationClassForCheckStateTransition(e,i){if(this._animationsDisabled)return"";switch(e){case vt.Init:if(i===vt.Checked)return this._animationClasses.uncheckedToChecked;if(i==vt.Indeterminate)return this._checked?this._animationClasses.checkedToIndeterminate:this._animationClasses.uncheckedToIndeterminate;break;case vt.Unchecked:return i===vt.Checked?this._animationClasses.uncheckedToChecked:this._animationClasses.uncheckedToIndeterminate;case vt.Checked:return i===vt.Unchecked?this._animationClasses.checkedToUnchecked:this._animationClasses.checkedToIndeterminate;case vt.Indeterminate:return i===vt.Checked?this._animationClasses.indeterminateToChecked:this._animationClasses.indeterminateToUnchecked}return""}_syncIndeterminate(e){let i=this._inputElement;i&&(i.nativeElement.indeterminate=e)}_onInputClick(){this._handleInputClick()}_preventBubblingFromLabel(e){e.target&&this._inputElement&&e.target!==this._inputElement.nativeElement&&e.stopPropagation()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){let e=["input"],i=["*"];return B({type:t,selectors:[["mat-checkbox"]],viewQuery:function(r,o){if(r&1&&Be(e,5),r&2){let s;M(s=T())&&(o._inputElement=s.first)}},hostAttrs:[1,"mat-mdc-checkbox"],hostVars:16,hostBindings:function(r,o){r&2&&(Gt("id",o.id),K("tabindex",null)("aria-label",null)("aria-labelledby",null),xt(o.color?"mat-"+o.color:"mat-accent"),S("_mat-animation-noopable",o._animationsDisabled)("mdc-checkbox--disabled",o.disabled)("mat-mdc-checkbox-disabled",o.disabled)("mat-mdc-checkbox-checked",o.checked)("mat-mdc-checkbox-disabled-interactive",o.disabledInteractive))},inputs:{ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],ariaExpanded:[2,"aria-expanded","ariaExpanded",I],ariaControls:[0,"aria-controls","ariaControls"],ariaOwns:[0,"aria-owns","ariaOwns"],id:"id",required:[2,"required","required",I],labelPosition:"labelPosition",name:"name",value:"value",disableRipple:[2,"disableRipple","disableRipple",I],tabIndex:[2,"tabIndex","tabIndex",a=>a==null?void 0:ht(a)],color:"color",disabledInteractive:[2,"disabledInteractive","disabledInteractive",I],checked:[2,"checked","checked",I],disabled:[2,"disabled","disabled",I],indeterminate:[2,"indeterminate","indeterminate",I]},outputs:{change:"change",indeterminateChange:"indeterminateChange"},exportAs:["matCheckbox"],features:[_e([{provide:Cn,useExisting:ze(()=>t),multi:!0},{provide:Ki,useExisting:t,multi:!0}]),rt],ngContentSelectors:i,decls:15,vars:23,consts:[["checkbox",""],["input",""],["label",""],["mat-internal-form-field","",3,"click","labelPosition","for"],[1,"mdc-checkbox"],["aria-hidden","true",1,"mat-mdc-checkbox-touch-target"],["type","checkbox",1,"mdc-checkbox__native-control",3,"blur","click","change","checked","indeterminate","disabled","id","required","tabIndex"],["aria-hidden","true",1,"mdc-checkbox__ripple"],["aria-hidden","true",1,"mdc-checkbox__background"],["focusable","false","viewBox","0 0 24 24",1,"mdc-checkbox__checkmark"],["fill","none","d","M1.73,12.91 8.1,19.28 22.79,4.59",1,"mdc-checkbox__checkmark-path"],[1,"mdc-checkbox__mixedmark"],["mat-ripple","","aria-hidden","true",1,"mat-mdc-checkbox-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mat-internal-form-field-label","mdc-label"]],template:function(r,o){if(r&1&&(he(),p(0,"label",3),C("click",function(c){return o._preventBubblingFromLabel(c)}),p(1,"span",4,0),$(3,"span",5),p(4,"input",6,1),C("blur",function(){return o._onBlur()})("click",function(){return o._onInputClick()})("change",function(c){return o._onInteractionEvent(c)}),h(),$(6,"span",7),p(7,"span",8),Ge(),p(8,"svg",9),$(9,"path",10),h(),nt(),$(10,"span",11),h(),$(11,"span",12),h(),p(12,"span",13,2),V(14),h()()),r&2){let s=et(2);F("labelPosition",o.labelPosition)("for",o.inputId),v(4),S("mdc-checkbox--selected",o.checked),F("checked",o.checked)("indeterminate",o.indeterminate)("disabled",o.disabled&&!o.disabledInteractive)("id",o.inputId)("required",o.required)("tabIndex",o.disabled&&!o.disabledInteractive?-1:o.tabIndex),K("aria-label",o.ariaLabel||null)("aria-labelledby",o.ariaLabelledby)("aria-describedby",o.ariaDescribedby)("aria-checked",o.indeterminate?"mixed":null)("aria-controls",o.ariaControls)("aria-disabled",o.disabled&&o.disabledInteractive?!0:null)("aria-expanded",o.ariaExpanded)("aria-owns",o.ariaOwns)("name",o.name)("value",o.value),v(7),F("matRippleTrigger",s)("matRippleDisabled",o.disableRipple||o.disabled)("matRippleCentered",!0)}},dependencies:[Gn,ul],styles:[`.mdc-checkbox {
  display: inline-block;
  position: relative;
  flex: 0 0 18px;
  box-sizing: content-box;
  width: 18px;
  height: 18px;
  line-height: 0;
  white-space: nowrap;
  cursor: pointer;
  vertical-align: bottom;
  padding: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  margin: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - var(--%NS%mat-checkbox-state-layer-size, 40px)) / 2);
}
.mdc-checkbox:hover > .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-unselected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-unselected-hover-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox:hover > .mat-mdc-checkbox-ripple > .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-unselected-hover-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control:focus + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-unselected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-unselected-focus-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control:focus ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-unselected-focus-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox:active > .mdc-checkbox__native-control + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-unselected-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-unselected-pressed-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox:active > .mdc-checkbox__native-control ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-unselected-pressed-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox:hover > .mdc-checkbox__native-control:checked + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-selected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-selected-hover-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox:hover > .mdc-checkbox__native-control:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-selected-hover-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox .mdc-checkbox__native-control:focus:checked + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-selected-focus-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox .mdc-checkbox__native-control:focus:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-selected-focus-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox:active > .mdc-checkbox__native-control:checked + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-selected-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-selected-pressed-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox:active > .mdc-checkbox__native-control:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-selected-pressed-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control ~ .mat-mdc-checkbox-ripple .mat-ripple-element,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control + .mdc-checkbox__ripple {
  background-color: var(--%NS%mat-checkbox-unselected-hover-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control {
  position: absolute;
  margin: 0;
  padding: 0;
  opacity: 0;
  cursor: inherit;
  z-index: 1;
  width: var(--%NS%mat-checkbox-state-layer-size, 40px);
  height: var(--%NS%mat-checkbox-state-layer-size, 40px);
  top: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - var(--%NS%mat-checkbox-state-layer-size, 40px)) / 2);
  right: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - var(--%NS%mat-checkbox-state-layer-size, 40px)) / 2);
  left: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - var(--%NS%mat-checkbox-state-layer-size, 40px)) / 2);
}

.mdc-checkbox--disabled {
  cursor: default;
  pointer-events: none;
}

.mdc-checkbox__background {
  display: inline-flex;
  position: absolute;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  width: 18px;
  height: 18px;
  border: 2px solid currentColor;
  border-radius: 2px;
  background-color: transparent;
  pointer-events: none;
  will-change: background-color, border-color;
  transition: background-color 90ms cubic-bezier(0.4, 0, 0.6, 1), border-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
  -webkit-print-color-adjust: exact;
  color-adjust: exact;
  border-color: var(--%NS%mat-checkbox-unselected-icon-color, var(--%NS%mat-sys-on-surface-variant));
  top: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  left: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - 18px) / 2);
}

.mdc-checkbox__native-control:enabled:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:enabled:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-selected-icon-color, var(--%NS%mat-sys-primary));
  background-color: var(--%NS%mat-checkbox-selected-icon-color, var(--%NS%mat-sys-primary));
}

.mdc-checkbox--disabled .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__background {
    border-color: GrayText;
  }
}

.mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--%NS%mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}
@media (forced-colors: active) {
  .mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
  .mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
    border-color: GrayText;
  }
}

.mdc-checkbox:hover > .mdc-checkbox__native-control:not(:checked) ~ .mdc-checkbox__background,
.mdc-checkbox:hover > .mdc-checkbox__native-control:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-unselected-hover-icon-color, var(--%NS%mat-sys-on-surface));
  background-color: transparent;
}

.mdc-checkbox:hover > .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox:hover > .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-selected-hover-icon-color, var(--%NS%mat-sys-primary));
  background-color: var(--%NS%mat-checkbox-selected-hover-icon-color, var(--%NS%mat-sys-primary));
}

.mdc-checkbox__native-control:focus:focus:not(:checked) ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:focus:focus:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-unselected-focus-icon-color, var(--%NS%mat-sys-on-surface));
}

.mdc-checkbox__native-control:focus:focus:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:focus:focus:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-selected-focus-icon-color, var(--%NS%mat-sys-primary));
  background-color: var(--%NS%mat-checkbox-selected-focus-icon-color, var(--%NS%mat-sys-primary));
}

.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
    border-color: GrayText;
  }
}
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--%NS%mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}

.mdc-checkbox__checkmark {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  width: 100%;
  opacity: 0;
  transition: opacity 180ms cubic-bezier(0.4, 0, 0.6, 1);
  color: var(--%NS%mat-checkbox-selected-checkmark-color, var(--%NS%mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mdc-checkbox__checkmark {
    color: CanvasText;
  }
}

.mdc-checkbox--disabled .mdc-checkbox__checkmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
  color: var(--%NS%mat-checkbox-disabled-selected-checkmark-color, var(--%NS%mat-sys-surface));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__checkmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
    color: GrayText;
  }
}

.mdc-checkbox__checkmark-path {
  transition: stroke-dashoffset 180ms cubic-bezier(0.4, 0, 0.6, 1);
  stroke: currentColor;
  stroke-width: 3.12px;
  stroke-dashoffset: 29.7833385;
  stroke-dasharray: 29.7833385;
}

.mdc-checkbox__mixedmark {
  width: 100%;
  height: 0;
  transform: scaleX(0) rotate(0deg);
  border-width: 1px;
  border-style: solid;
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
  border-color: var(--%NS%mat-checkbox-selected-checkmark-color, var(--%NS%mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mdc-checkbox__mixedmark {
    margin: 0 1px;
  }
}

.mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
  border-color: var(--%NS%mat-checkbox-disabled-selected-checkmark-color, var(--%NS%mat-sys-surface));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
    border-color: GrayText;
  }
}

.mdc-checkbox--anim-unchecked-checked .mdc-checkbox__background,
.mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__background,
.mdc-checkbox--anim-checked-unchecked .mdc-checkbox__background,
.mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__background {
  animation-duration: 180ms;
  animation-timing-function: linear;
}

.mdc-checkbox--anim-unchecked-checked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-unchecked-checked-checkmark-path 180ms linear;
  transition: none;
}

.mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-unchecked-indeterminate-mixedmark 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-checked-unchecked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-checked-unchecked-checkmark-path 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__checkmark {
  animation: mdc-checkbox-checked-indeterminate-checkmark 90ms linear;
  transition: none;
}
.mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-checked-indeterminate-mixedmark 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__checkmark {
  animation: mdc-checkbox-indeterminate-checked-checkmark 500ms linear;
  transition: none;
}
.mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-checked-mixedmark 500ms linear;
  transition: none;
}

.mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-unchecked-mixedmark 300ms linear;
  transition: none;
}

.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path {
  stroke-dashoffset: 0;
}

.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transition: opacity 180ms cubic-bezier(0, 0, 0.2, 1), transform 180ms cubic-bezier(0, 0, 0.2, 1);
  opacity: 1;
}
.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(-45deg);
}

.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transform: rotate(45deg);
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(0deg);
  opacity: 1;
}

@keyframes mdc-checkbox-unchecked-checked-checkmark-path {
  0%, 50% {
    stroke-dashoffset: 29.7833385;
  }
  50% {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
  }
  100% {
    stroke-dashoffset: 0;
  }
}
@keyframes mdc-checkbox-unchecked-indeterminate-mixedmark {
  0%, 68.2% {
    transform: scaleX(0);
  }
  68.2% {
    animation-timing-function: cubic-bezier(0, 0, 0, 1);
  }
  100% {
    transform: scaleX(1);
  }
}
@keyframes mdc-checkbox-checked-unchecked-checkmark-path {
  from {
    animation-timing-function: cubic-bezier(0.4, 0, 1, 1);
    opacity: 1;
    stroke-dashoffset: 0;
  }
  to {
    opacity: 0;
    stroke-dashoffset: -29.7833385;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-checkmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(45deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-checkmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(45deg);
    opacity: 0;
  }
  to {
    transform: rotate(360deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(-45deg);
    opacity: 0;
  }
  to {
    transform: rotate(0deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(315deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-unchecked-mixedmark {
  0% {
    animation-timing-function: linear;
    transform: scaleX(1);
    opacity: 1;
  }
  32.8%, 100% {
    transform: scaleX(0);
    opacity: 0;
  }
}
.mat-mdc-checkbox {
  display: inline-block;
  position: relative;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mat-mdc-checkbox-touch-target,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__native-control,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__ripple,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mat-mdc-checkbox-ripple::before,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-checkbox label {
  cursor: pointer;
}
.mat-mdc-checkbox .mat-internal-form-field {
  color: var(--%NS%mat-checkbox-label-text-color, var(--%NS%mat-sys-on-surface));
  font-family: var(--%NS%mat-checkbox-label-text-font, var(--%NS%mat-sys-body-medium-font));
  line-height: var(--%NS%mat-checkbox-label-text-line-height, var(--%NS%mat-sys-body-medium-line-height));
  font-size: var(--%NS%mat-checkbox-label-text-size, var(--%NS%mat-sys-body-medium-size));
  letter-spacing: var(--%NS%mat-checkbox-label-text-tracking, var(--%NS%mat-sys-body-medium-tracking));
  font-weight: var(--%NS%mat-checkbox-label-text-weight, var(--%NS%mat-sys-body-medium-weight));
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled.mat-mdc-checkbox-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled.mat-mdc-checkbox-disabled-interactive input {
  cursor: default;
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled label {
  cursor: default;
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled .mat-internal-form-field-label {
  color: var(--%NS%mat-checkbox-disabled-label-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-checkbox.mat-mdc-checkbox-disabled .mat-internal-form-field-label {
    color: GrayText;
  }
}
.mat-mdc-checkbox .mat-internal-form-field-label:empty {
  display: none;
}
.mat-mdc-checkbox .mdc-checkbox__ripple {
  opacity: 0;
}

.mat-mdc-checkbox .mat-mdc-checkbox-ripple,
.mdc-checkbox__ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
}
.mat-mdc-checkbox .mat-mdc-checkbox-ripple:not(:empty),
.mdc-checkbox__ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-mdc-checkbox-ripple .mat-ripple-element {
  opacity: 0.1;
}

.mat-mdc-checkbox-touch-target {
  position: absolute;
  top: 50%;
  left: 50%;
  height: var(--%NS%mat-checkbox-touch-target-size, 48px);
  width: var(--%NS%mat-checkbox-touch-target-size, 48px);
  transform: translate(-50%, -50%);
  display: var(--%NS%mat-checkbox-touch-target-display, block);
}

.mat-mdc-checkbox .mat-mdc-checkbox-ripple::before {
  border-radius: 50%;
}

.mdc-checkbox__native-control:focus-visible ~ .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2})})()}return t})(),nx=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=G({imports:[Gp,me]})}return t})();var pl=class{source;value;constructor(n,e){this.source=n,this.value=e}},SN={provide:Cn,useExisting:ze(()=>Wp),multi:!0},ix=new y("MatRadioGroup"),EN=new y("mat-radio-default-options",{providedIn:"root",factory:()=>({color:"accent",disabledInteractive:!1})}),Wp=(()=>{class t{_changeDetector=m(Ie);_value=null;_name=m(Pe).getId("mat-radio-group-");_selected=null;_isInitialized=!1;_labelPosition="after";_disabled=!1;_required=!1;_buttonChanges;_controlValueAccessorChangeFn=()=>{};onTouched=()=>{};change=new z;_radios;color;get name(){return this._name}set name(e){this._name=e,this._updateRadioButtonNames()}get labelPosition(){return this._labelPosition}set labelPosition(e){this._labelPosition=e==="before"?"before":"after",this._markRadiosForCheck()}get value(){return this._value}set value(e){this._value!==e&&(this._value=e,this._updateSelectedRadioFromValue(),this._checkSelectedRadioButton())}_checkSelectedRadioButton(){this._selected&&!this._selected.checked&&(this._selected.checked=!0)}get selected(){return this._selected}set selected(e){this._selected=e,this.value=e?e.value:null,this._checkSelectedRadioButton()}get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._markRadiosForCheck()}get required(){return this._required}set required(e){this._required=e,this._markRadiosForCheck()}get disabledInteractive(){return this._disabledInteractive}set disabledInteractive(e){this._disabledInteractive=e,this._markRadiosForCheck()}_disabledInteractive=!1;ngAfterContentInit(){this._isInitialized=!0,this._buttonChanges=this._radios.changes.subscribe(()=>{this.selected&&!this._radios.find(e=>e===this.selected)&&(this._selected=null)})}ngOnDestroy(){this._buttonChanges?.unsubscribe()}_touch(){this.onTouched&&this.onTouched()}_updateRadioButtonNames(){this._radios&&this._radios.forEach(e=>{e.name=this.name,e._markForCheck()})}_updateSelectedRadioFromValue(){let e=this._selected!==null&&this._selected.value===this._value;this._radios&&!e&&(this._selected=null,this._radios.forEach(i=>{i.checked=this.value===i.value,i.checked&&(this._selected=i)}))}_emitChangeEvent(){this._isInitialized&&this.change.emit(new pl(this._selected,this._value))}_markRadiosForCheck(){this._radios&&this._radios.forEach(e=>e._markForCheck())}writeValue(e){this.value=e,this._changeDetector.markForCheck()}registerOnChange(e){this._controlValueAccessorChangeFn=e}registerOnTouched(e){this.onTouched=e}setDisabledState(e){this.disabled=e,this._changeDetector.markForCheck()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["mat-radio-group"]],contentQueries:function(i,a,r){if(i&1&&Ye(r,hl,5),i&2){let o;M(o=T())&&(a._radios=o)}},hostAttrs:["role","radiogroup",1,"mat-mdc-radio-group"],inputs:{color:"color",name:"name",labelPosition:"labelPosition",value:"value",selected:"selected",disabled:[2,"disabled","disabled",I],required:[2,"required","required",I],disabledInteractive:[2,"disabledInteractive","disabledInteractive",I]},outputs:{change:"change"},exportAs:["matRadioGroup"],features:[_e([SN,{provide:ix,useExisting:t}])]})}return t})(),hl=(()=>{class t{_elementRef=m(L);_changeDetector=m(Ie);_focusMonitor=m(on);_radioDispatcher=m(Pp);_defaultOptions=m(EN,{optional:!0});_ngZone=m(N);_renderer=m(Se);_uniqueId=m(Pe).getId("mat-radio-");_cleanupClick;id=this._uniqueId;name;ariaLabel;ariaLabelledby;ariaDescribedby;disableRipple=!1;tabIndex=0;get checked(){return this._checked}set checked(e){this._checked!==e&&(this._checked=e,e&&this.radioGroup&&this.radioGroup.value!==this.value?this.radioGroup.selected=this:!e&&this.radioGroup&&this.radioGroup.value===this.value&&(this.radioGroup.selected=null),e&&this._radioDispatcher.notify(this.id,this.name),this._changeDetector.markForCheck())}get value(){return this._value}set value(e){this._value!==e&&(this._value=e,this.radioGroup!==null&&(this.checked||(this.checked=this.radioGroup.value===e),this.checked&&(this.radioGroup.selected=this)))}get labelPosition(){return this._labelPosition||this.radioGroup&&this.radioGroup.labelPosition||"after"}set labelPosition(e){this._labelPosition=e}_labelPosition;get disabled(){return this._disabled||this.radioGroup!==null&&this.radioGroup.disabled}set disabled(e){this._setDisabled(e)}get required(){return this._required||this.radioGroup&&this.radioGroup.required}set required(e){e!==this._required&&this._changeDetector.markForCheck(),this._required=e}get color(){return this._color||this.radioGroup&&this.radioGroup.color||this._defaultOptions&&this._defaultOptions.color||"accent"}set color(e){this._color=e}_color;get disabledInteractive(){return this._disabledInteractive||this.radioGroup!==null&&this.radioGroup.disabledInteractive}set disabledInteractive(e){this._disabledInteractive=e}_disabledInteractive;change=new z;radioGroup;get inputId(){return`${this.id||this._uniqueId}-input`}_checked=!1;_disabled=!1;_required=!1;_value=null;_removeUniqueSelectionListener=()=>{};_previousTabIndex;_inputElement;_rippleTrigger;_noopAnimations=Re();_injector=m(X);constructor(){m(je).load(Ct);let e=m(ix,{optional:!0}),i=m(new Un("tabindex"),{optional:!0});this.radioGroup=e,this._disabledInteractive=this._defaultOptions?.disabledInteractive??!1,i&&(this.tabIndex=ht(i,0))}focus(e,i){i?this._focusMonitor.focusVia(this._inputElement,i,e):this._inputElement.nativeElement.focus(e)}_markForCheck(){this._changeDetector.markForCheck()}ngOnInit(){this.radioGroup&&(this.checked=this.radioGroup.value===this._value,this.checked&&(this.radioGroup.selected=this),this.name=this.radioGroup.name),this._removeUniqueSelectionListener=this._radioDispatcher.listen((e,i)=>{e!==this.id&&i===this.name&&(this.checked=!1)})}ngDoCheck(){this._updateTabIndex()}ngAfterViewInit(){this._updateTabIndex(),this._focusMonitor.monitor(this._elementRef,!0).subscribe(e=>{!e&&this.radioGroup&&this.radioGroup._touch()}),this._ngZone.runOutsideAngular(()=>{this._cleanupClick=this._renderer.listen(this._inputElement.nativeElement,"click",this._onInputClick)})}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._removeUniqueSelectionListener()}_emitChangeEvent(){this.change.emit(new pl(this,this._value))}_isRippleDisabled(){return this.disableRipple||this.disabled}_onInputInteraction(e){if(e.stopPropagation(),!this.checked&&!this.disabled){let i=this.radioGroup&&this.value!==this.radioGroup.value;this.checked=!0,this._emitChangeEvent(),this.radioGroup&&(this.radioGroup._controlValueAccessorChangeFn(this.value),i&&this.radioGroup._emitChangeEvent())}}_setDisabled(e){this._disabled!==e&&(this._disabled=e,this._changeDetector.markForCheck())}_onInputClick=e=>{this.disabled&&this.disabledInteractive&&e.preventDefault()};_updateTabIndex(){let e=this.radioGroup,i;if(!e||!e.selected||this.disabled?i=this.tabIndex:i=e.selected===this?this.tabIndex:-1,i!==this._previousTabIndex){let a=this._inputElement?.nativeElement;a&&(a.setAttribute("tabindex",i+""),this._previousTabIndex=i,mt(()=>{queueMicrotask(()=>{e&&e.selected&&e.selected!==this&&document.activeElement===a&&(e.selected?._inputElement.nativeElement.focus(),document.activeElement===a&&this._inputElement.nativeElement.blur())})},{injector:this._injector}))}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){let e=["input"],i=["formField"];return B({type:t,selectors:[["mat-radio-button"]],viewQuery:function(o,s){if(o&1&&Be(e,5)(i,7,L),o&2){let c;M(c=T())&&(s._inputElement=c.first),M(c=T())&&(s._rippleTrigger=c.first)}},hostAttrs:[1,"mat-mdc-radio-button"],hostVars:19,hostBindings:function(o,s){o&1&&C("focus",function(){return s._inputElement.nativeElement.focus()}),o&2&&(K("id",s.id)("tabindex",null)("aria-label",null)("aria-labelledby",null)("aria-describedby",null),S("mat-primary",s.color==="primary")("mat-accent",s.color==="accent")("mat-warn",s.color==="warn")("mat-mdc-radio-checked",s.checked)("mat-mdc-radio-disabled",s.disabled)("mat-mdc-radio-disabled-interactive",s.disabledInteractive)("_mat-animation-noopable",s._noopAnimations))},inputs:{id:"id",name:"name",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],disableRipple:[2,"disableRipple","disableRipple",I],tabIndex:[2,"tabIndex","tabIndex",r=>r==null?0:ht(r)],checked:[2,"checked","checked",I],value:"value",labelPosition:"labelPosition",disabled:[2,"disabled","disabled",I],required:[2,"required","required",I],color:"color",disabledInteractive:[2,"disabledInteractive","disabledInteractive",I]},outputs:{change:"change"},exportAs:["matRadioButton"],ngContentSelectors:["*"],decls:13,vars:17,consts:[["formField",""],["input",""],["mat-internal-form-field","",3,"labelPosition","for"],[1,"mdc-radio"],[1,"mat-mdc-radio-touch-target"],["type","radio","aria-invalid","false",1,"mdc-radio__native-control",3,"change","id","checked","disabled","required"],["aria-hidden","true",1,"mdc-radio__background"],[1,"mdc-radio__outer-circle"],[1,"mdc-radio__inner-circle"],["mat-ripple","","aria-hidden","true",1,"mat-radio-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mat-ripple-element","mat-radio-persistent-ripple"],[1,"mat-internal-form-field-label","mdc-label"]],template:function(o,s){o&1&&(he(),p(0,"label",2,0)(2,"span",3),$(3,"span",4),p(4,"input",5,1),C("change",function(l){return s._onInputInteraction(l)}),h(),p(6,"span",6),$(7,"span",7)(8,"span",8),h(),p(9,"span",9),$(10,"span",10),h()(),p(11,"span",11),V(12),h()()),o&2&&(F("labelPosition",s.labelPosition)("for",s.inputId),v(2),S("mdc-radio--disabled",s.disabled),v(2),F("id",s.inputId)("checked",s.checked)("disabled",s.disabled&&!s.disabledInteractive)("required",s.required),K("name",s.name)("value",s.value)("aria-label",s.ariaLabel)("aria-labelledby",s.ariaLabelledby)("aria-describedby",s.ariaDescribedby)("aria-disabled",s.disabled&&s.disabledInteractive?"true":null),v(5),F("matRippleTrigger",s._rippleTrigger.nativeElement)("matRippleDisabled",s._isRippleDisabled())("matRippleCentered",!0))},dependencies:[Gn,ul],styles:[`.mat-mdc-radio-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-radio-button .mdc-radio {
  display: inline-block;
  position: relative;
  flex: 0 0 auto;
  box-sizing: content-box;
  width: 20px;
  height: 20px;
  will-change: opacity, transform, border-color, color;
  padding: calc((var(--%NS%mat-radio-state-layer-size, 40px) - 20px) / 2);
  cursor: pointer;
}
.mat-mdc-radio-button .mdc-radio:hover > .mdc-radio__native-control:not([disabled]):not(:focus) ~ .mdc-radio__background::before {
  opacity: 0.04;
  transform: scale(1);
}
.mat-mdc-radio-button .mdc-radio:hover > .mdc-radio__native-control:not([disabled]) ~ .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--%NS%mat-radio-unselected-hover-icon-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-radio-button .mdc-radio:hover > .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--%NS%mat-radio-selected-hover-icon-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-radio-button .mdc-radio:hover > .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--%NS%mat-radio-selected-hover-icon-color, var(--%NS%mat-sys-primary, currentColor));
}
.mat-mdc-radio-button .mdc-radio:active > .mdc-radio__native-control:enabled:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--%NS%mat-radio-unselected-pressed-icon-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-radio-button .mdc-radio:active > .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--%NS%mat-radio-selected-pressed-icon-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-radio-button .mdc-radio:active > .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--%NS%mat-radio-selected-pressed-icon-color, var(--%NS%mat-sys-primary, currentColor));
}
.mat-mdc-radio-button .mdc-radio__background {
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  width: 20px;
  height: 20px;
}
.mat-mdc-radio-button .mdc-radio__background::before {
  position: absolute;
  transform: scale(0, 0);
  border-radius: 50%;
  opacity: 0;
  pointer-events: none;
  content: "";
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
  width: var(--%NS%mat-radio-state-layer-size, 40px);
  height: var(--%NS%mat-radio-state-layer-size, 40px);
  top: calc(-1 * (var(--%NS%mat-radio-state-layer-size, 40px) - 20px) / 2);
  left: calc(-1 * (var(--%NS%mat-radio-state-layer-size, 40px) - 20px) / 2);
}
.mat-mdc-radio-button .mdc-radio__outer-circle {
  position: absolute;
  top: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  border-width: 2px;
  border-style: solid;
  border-radius: 50%;
  transition: border-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mat-mdc-radio-button .mdc-radio__inner-circle {
  position: absolute;
  top: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  transform: scale(0);
  border-radius: 50%;
  transition: transform 90ms cubic-bezier(0.4, 0, 0.6, 1), background-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
@media (forced-colors: active) {
  .mat-mdc-radio-button .mdc-radio__inner-circle {
    background-color: CanvasText !important;
  }
}
.mat-mdc-radio-button .mdc-radio__native-control {
  position: absolute;
  margin: 0;
  padding: 0;
  opacity: 0;
  top: 0;
  right: 0;
  left: 0;
  cursor: inherit;
  z-index: 1;
  width: var(--%NS%mat-radio-state-layer-size, 40px);
  height: var(--%NS%mat-radio-state-layer-size, 40px);
}
.mat-mdc-radio-button .mdc-radio__native-control:checked + .mdc-radio__background, .mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background {
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 1), transform 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__outer-circle, .mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__outer-circle {
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__inner-circle, .mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__inner-circle {
  transition: transform 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button .mdc-radio__native-control:focus + .mdc-radio__background::before {
  transform: scale(1);
  opacity: 0.12;
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 1), transform 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button .mdc-radio__native-control:disabled:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--%NS%mat-radio-disabled-unselected-icon-color, var(--%NS%mat-sys-on-surface));
  opacity: var(--%NS%mat-radio-disabled-unselected-icon-opacity, 0.38);
}
.mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background {
  cursor: default;
}
.mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--%NS%mat-radio-disabled-selected-icon-color, var(--%NS%mat-sys-on-surface));
  opacity: var(--%NS%mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--%NS%mat-radio-disabled-selected-icon-color, var(--%NS%mat-sys-on-surface, currentColor));
  opacity: var(--%NS%mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--%NS%mat-radio-unselected-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--%NS%mat-radio-selected-icon-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--%NS%mat-radio-selected-icon-color, var(--%NS%mat-sys-primary, currentColor));
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:focus:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--%NS%mat-radio-selected-focus-icon-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:focus:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--%NS%mat-radio-selected-focus-icon-color, var(--%NS%mat-sys-primary, currentColor));
}
.mat-mdc-radio-button .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  transform: scale(0.5);
  transition: transform 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled {
  pointer-events: auto;
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--%NS%mat-radio-disabled-unselected-icon-color, var(--%NS%mat-sys-on-surface));
  opacity: var(--%NS%mat-radio-disabled-unselected-icon-opacity, 0.38);
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--%NS%disabled:hover .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__outer-circle,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control:checked:focus + .mdc-radio__background > .mdc-radio__outer-circle,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--%NS%mat-radio-disabled-selected-icon-color, var(--%NS%mat-sys-on-surface));
  opacity: var(--%NS%mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--%NS%disabled:hover .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__inner-circle,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control:checked:focus + .mdc-radio__background > .mdc-radio__inner-circle,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--%NS%mat-radio-disabled-selected-icon-color, var(--%NS%mat-sys-on-surface, currentColor));
  opacity: var(--%NS%mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-radio-button._mat-animation-noopable .mdc-radio__background::before,
.mat-mdc-radio-button._mat-animation-noopable .mdc-radio__outer-circle,
.mat-mdc-radio-button._mat-animation-noopable .mdc-radio__inner-circle {
  transition: none !important;
}
.mat-mdc-radio-button .mat-internal-form-field-label:empty {
  display: none;
}
.mat-mdc-radio-button .mdc-radio__background::before {
  background-color: var(--%NS%mat-radio-ripple-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-radio-button.mat-mdc-radio-checked .mat-ripple-element,
.mat-mdc-radio-button.mat-mdc-radio-checked .mdc-radio__background::before {
  background-color: var(--%NS%mat-radio-checked-ripple-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mat-ripple-element,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__background::before {
  background-color: var(--%NS%mat-radio-ripple-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-radio-button .mat-internal-form-field {
  color: var(--%NS%mat-radio-label-text-color, var(--%NS%mat-sys-on-surface));
  font-family: var(--%NS%mat-radio-label-text-font, var(--%NS%mat-sys-body-medium-font));
  line-height: var(--%NS%mat-radio-label-text-line-height, var(--%NS%mat-sys-body-medium-line-height));
  font-size: var(--%NS%mat-radio-label-text-size, var(--%NS%mat-sys-body-medium-size));
  letter-spacing: var(--%NS%mat-radio-label-text-tracking, var(--%NS%mat-sys-body-medium-tracking));
  font-weight: var(--%NS%mat-radio-label-text-weight, var(--%NS%mat-sys-body-medium-weight));
  cursor: pointer;
}
.mat-mdc-radio-button .mdc-radio--disabled + .mat-internal-form-field-label {
  color: var(--%NS%mat-radio-disabled-label-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-radio-button .mat-radio-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: 50%;
}
.mat-mdc-radio-button .mat-radio-ripple > .mat-ripple-element {
  opacity: 0.14;
}
.mat-mdc-radio-button .mat-radio-ripple::before {
  border-radius: 50%;
}
.mat-mdc-radio-button .mdc-radio > .mdc-radio__native-control:focus:enabled:not(:checked) ~ .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--%NS%mat-radio-unselected-focus-icon-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-radio-button.cdk-focused .mat-focus-indicator::before {
  content: "";
}

.mat-mdc-radio-disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-radio-disabled.mat-mdc-radio-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-radio-touch-target {
  position: absolute;
  top: 50%;
  left: 50%;
  height: var(--%NS%mat-radio-touch-target-size, 48px);
  width: var(--%NS%mat-radio-touch-target-size, 48px);
  transform: translate(-50%, -50%);
  display: var(--%NS%mat-radio-touch-target-display, block);
}
[dir=rtl] .mat-mdc-radio-touch-target {
  left: auto;
  right: 50%;
  transform: translate(50%, -50%);
}
`],encapsulation:2})})()}return t})(),ax=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=G({imports:[Sn,hl,me]})}return t})();var lo=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},Ya=class extends lo{component;viewContainerRef;injector;projectableNodes;bindings;directives;constructor(n,e,i,a,r,o){super(),this.component=n,this.viewContainerRef=e,this.injector=i,this.projectableNodes=a,this.bindings=r||null,this.directives=o||null}},Wn=class extends lo{templateRef;viewContainerRef;context;injector;constructor(n,e,i,a){super(),this.templateRef=n,this.viewContainerRef=e,this.context=i,this.injector=a}get origin(){return this.templateRef.elementRef}attach(n,e=this.context){return this.context=e,super.attach(n)}detach(){return this.context=void 0,super.detach()}},qp=class extends lo{element;constructor(n){super(),this.element=n instanceof L?n.nativeElement:n}},Za=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof Ya)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof Wn)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof qp)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},fl=class extends Za{outletElement;_appRef;_defaultInjector;constructor(n,e,i){super(),this.outletElement=n,this._appRef=e,this._defaultInjector=i}attachComponentPortal(n){let e;if(n.viewContainerRef){let i=n.injector||n.viewContainerRef.injector,a=i.get(ai,null,{optional:!0})||void 0;e=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:i,ngModuleRef:a,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),this.setDisposeFn(()=>e.destroy())}else{let i=this._appRef,a=n.injector||this._defaultInjector||X.NULL,r=a.get(ct,i.injector);e=bc(n.component,{elementInjector:a,environmentInjector:r,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),i.attachView(e.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(e.hostView),e.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(e)),this._attachedPortal=n,e}attachTemplatePortal(n){let e=n.viewContainerRef,i=e.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return i.rootNodes.forEach(a=>this.outletElement.appendChild(a)),i.detectChanges(),this.setDisposeFn(()=>{let a=e.indexOf(i);a!==-1&&e.remove(a)}),this._attachedPortal=n,i}attachDomPortal=n=>{let e=n.element;e.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");e.parentNode.insertBefore(i,e),this.outletElement.appendChild(e),this._attachedPortal=n,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(e,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}},rx=(()=>{class t extends Wn{constructor(){let e=m(bt),i=m(Vt);super(e,i)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["","cdkPortal",""]],exportAs:["cdkPortal"],features:[le]})}return t})(),Ka=(()=>{class t extends Za{_moduleRef=m(ai,{optional:!0});_document=m(ee);_viewContainerRef=m(Vt);_isInitialized=!1;_attachedRef=null;get portal(){return this._attachedPortal}set portal(e){this.hasAttached()&&!e&&!this._isInitialized||(this.hasAttached()&&super.detach(),e&&super.attach(e),this._attachedPortal=e||null)}attached=new z;get attachedRef(){return this._attachedRef}ngOnInit(){this._isInitialized=!0}ngOnDestroy(){super.dispose(),this._attachedRef=this._attachedPortal=null}attachComponentPortal(e){e.setAttachedHost(this);let i=e.viewContainerRef!=null?e.viewContainerRef:this._viewContainerRef,a=i.createComponent(e.component,{index:i.length,injector:e.injector||i.injector,projectableNodes:e.projectableNodes||void 0,ngModuleRef:this._moduleRef||void 0,bindings:e.bindings||void 0,directives:e.directives||void 0});return i!==this._viewContainerRef&&this._insertContent(a.hostView.rootNodes[0]),super.setDisposeFn(()=>a.destroy()),this._attachedPortal=e,this._attachedRef=a,this.attached.emit(a),a}attachTemplatePortal(e){e.setAttachedHost(this);let i=this._viewContainerRef.createEmbeddedView(e.templateRef,e.context,{injector:e.injector});return super.setDisposeFn(()=>this._viewContainerRef.clear()),this._attachedPortal=e,this._attachedRef=i,this.attached.emit(i),i}attachDomPortal=e=>{let i=e.element;i.parentNode;let a=this._document.createComment("dom-portal");e.setAttachedHost(this),i.parentNode.insertBefore(a,i),this._insertContent(i),this._attachedPortal=e,super.setDisposeFn(()=>a.parentNode?.replaceChild(i,a))};_insertContent(e){let i=this._viewContainerRef.element.nativeElement;i.nodeType===i.ELEMENT_NODE?i.appendChild(e):i.parentNode?.insertBefore(e,i)}static \u0275fac=(()=>{let e;return function(a){return(e||(e=Fe(t)))(a||t)}})();static \u0275dir=D({type:t,selectors:[["","cdkPortalOutlet",""]],inputs:{portal:[0,"cdkPortalOutlet","portal"]},outputs:{attached:"attached"},exportAs:["cdkPortalOutlet"],features:[le]})}return t})(),gl=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=G({})}return t})();var IN=20,bl=(()=>{class t{_ngZone=m(N);_platform=m(ye);_renderer=m(Je).createRenderer(null,null);_cleanupGlobalListener;_scrolled=new E;_scrolledCount=0;scrollContainers=new Map;register(e){this.scrollContainers.has(e)||this.scrollContainers.set(e,e.elementScrolled().subscribe(()=>this._scrolled.next(e)))}deregister(e){let i=this.scrollContainers.get(e);i&&(i.unsubscribe(),this.scrollContainers.delete(e))}scrolled(e=IN){return this._platform.isBrowser?new se(i=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let a=e>0?this._scrolled.pipe(Zo(e)).subscribe(i):this._scrolled.subscribe(i);return this._scrolledCount++,()=>{a.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):In()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((e,i)=>this.deregister(i)),this._scrolled.complete()}ancestorScrolled(e,i){let a=this.getAncestorScrollContainers(e);return this.scrolled(i).pipe(ot(r=>!r||a.indexOf(r)>-1))}getAncestorScrollContainers(e){let i=[];return this.scrollContainers.forEach((a,r)=>{this._targetContainsElement(r,e)&&i.push(r)}),i}_targetContainsElement(e,i){let a=wt(i),r=e.getElementRef().nativeElement;do if(a==r)return!0;while(a=a.parentElement);return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})}return t})(),sx=(()=>{class t{elementRef=m(L);scrollDispatcher=m(bl);ngZone=m(N);dir=m(Mt,{optional:!0});_scrollElement=this.elementRef.nativeElement;_destroyed=new E;_renderer=m(Se);_cleanupScroll;_elementScrolled=new E;ngOnInit(){this._cleanupScroll=this.ngZone.runOutsideAngular(()=>this._renderer.listen(this._scrollElement,"scroll",e=>this._elementScrolled.next(e))),this.scrollDispatcher.register(this)}ngOnDestroy(){this._cleanupScroll?.(),this._elementScrolled.complete(),this.scrollDispatcher.deregister(this),this._destroyed.next(),this._destroyed.complete()}elementScrolled(){return this._elementScrolled}getElementRef(){return this.elementRef}scrollTo(e){let i=this.elementRef.nativeElement,a=this.dir&&this.dir.value=="rtl";e.left==null&&(e.left=a?e.end:e.start),e.right==null&&(e.right=a?e.start:e.end),e.bottom!=null&&(e.top=i.scrollHeight-i.clientHeight-e.bottom),a&&Ga()!=sn.NORMAL?(e.left!=null&&(e.right=i.scrollWidth-i.clientWidth-e.left),Ga()==sn.INVERTED?e.left=e.right:Ga()==sn.NEGATED&&(e.left=e.right?-e.right:e.right)):e.right!=null&&(e.left=i.scrollWidth-i.clientWidth-e.right),this._applyScrollToOptions(e)}_applyScrollToOptions(e){let i=this.elementRef.nativeElement;Kc()?i.scrollTo(e):(e.top!=null&&(i.scrollTop=e.top),e.left!=null&&(i.scrollLeft=e.left))}measureScrollOffset(e){let i="left",a="right",r=this.elementRef.nativeElement;if(e=="top")return r.scrollTop;if(e=="bottom")return r.scrollHeight-r.clientHeight-r.scrollTop;let o=this.dir&&this.dir.value=="rtl";return e=="start"?e=o?a:i:e=="end"&&(e=o?i:a),o&&Ga()==sn.INVERTED?e==i?r.scrollWidth-r.clientWidth-r.scrollLeft:r.scrollLeft:o&&Ga()==sn.NEGATED?e==i?r.scrollLeft+r.scrollWidth-r.clientWidth:-r.scrollLeft:e==i?r.scrollLeft:r.scrollWidth-r.clientWidth-r.scrollLeft}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["","cdk-scrollable",""],["","cdkScrollable",""]]})}return t})(),NN=20,na=(()=>{class t{_platform=m(ye);_listeners;_viewportSize=null;_change=new E;_document=m(ee);constructor(){let e=m(N),i=m(Je).createRenderer(null,null);e.runOutsideAngular(()=>{if(this._platform.isBrowser){let a=r=>this._change.next(r);this._listeners=[i.listen("window","resize",a),i.listen("window","orientationchange",a)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(e=>e()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let e={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),e}getViewportRect(){let e=this.getViewportScrollPosition(),{width:i,height:a}=this.getViewportSize();return{top:e.top,left:e.left,bottom:e.top+a,right:e.left+i,height:a,width:i}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let e=this._document,i=this._getWindow(),a=e.documentElement,r=a.getBoundingClientRect(),o=-r.top||e.body?.scrollTop||i.scrollY||a.scrollTop||0,s=-r.left||e.body?.scrollLeft||i.scrollX||a.scrollLeft||0;return{top:o,left:s}}change(e=NN){return e>0?this._change.pipe(Zo(e)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let e=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:e.innerWidth,height:e.innerHeight}:{width:0,height:0}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})}return t})();var ox=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=G({})}return t})(),Yp=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=G({imports:[me,ox,me,ox]})}return t})();var MN=new y("MatTabContent"),TN=(()=>{class t{template=m(bt);static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["","matTabContent",""]],features:[_e([{provide:MN,useExisting:t}])]})}return t})(),AN=new y("MatTabLabel"),mx=new y("MAT_TAB"),RN=(()=>{class t extends rx{_closestTab=m(mx,{optional:!0});static \u0275fac=(()=>{let e;return function(a){return(e||(e=Fe(t)))(a||t)}})();static \u0275dir=D({type:t,selectors:[["","mat-tab-label",""],["","matTabLabel",""]],features:[_e([{provide:AN,useExisting:t}]),le]})}return t})(),ux=new y("MAT_TAB_GROUP"),eh=(()=>{class t{_viewContainerRef=m(Vt);_closestTabGroup=m(ux,{optional:!0});disabled=!1;get templateLabel(){return this._templateLabel}set templateLabel(e){this._setTemplateLabelInput(e)}_templateLabel;_explicitContent=void 0;_implicitContent;textLabel="";ariaLabel;ariaLabelledby;labelClass;bodyClass;id=null;_contentPortal=null;get content(){return this._contentPortal}_stateChanges=new E;position=null;origin=null;isActive=!1;constructor(){m(je).load(Ct)}ngOnChanges(e){(Object.hasOwn(e,"textLabel")||Object.hasOwn(e,"disabled"))&&this._stateChanges.next()}ngOnDestroy(){this._stateChanges.complete()}ngOnInit(){this._contentPortal=new Wn(this._explicitContent||this._implicitContent,this._viewContainerRef)}_setTemplateLabelInput(e){e&&e._closestTab===this&&(this._templateLabel=e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){let e=["*"];function i(a,r){a&1&&V(0)}return B({type:t,selectors:[["mat-tab"]],contentQueries:function(r,o,s){if(r&1&&Ye(s,RN,5)(s,TN,7,bt),r&2){let c;M(c=T())&&(o.templateLabel=c.first),M(c=T())&&(o._explicitContent=c.first)}},viewQuery:function(r,o){if(r&1&&Be(bt,7),r&2){let s;M(s=T())&&(o._implicitContent=s.first)}},hostAttrs:["hidden",""],hostVars:1,hostBindings:function(r,o){r&2&&K("id",null)},inputs:{disabled:[2,"disabled","disabled",I],textLabel:[0,"label","textLabel"],ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],labelClass:"labelClass",bodyClass:"bodyClass",id:"id"},exportAs:["matTab"],features:[_e([{provide:mx,useExisting:t}]),rt],ngContentSelectors:e,decls:1,vars:0,template:function(r,o){r&1&&(he(),cc(0,i,1,0,"ng-template"))},encapsulation:2,changeDetection:1})})()}return t})(),Zp="mdc-tab-indicator--active",cx="mdc-tab-indicator--no-transition",Qp=class{_items;_currentItem;constructor(n){this._items=n}hide(){this._items.forEach(n=>n.deactivateInkBar()),this._currentItem=void 0}alignToElement(n){let e=this._items.find(a=>a.elementRef.nativeElement===n),i=this._currentItem;if(e!==i&&(i?.deactivateInkBar(),e)){let a=i?.elementRef.nativeElement.getBoundingClientRect?.();e.activateInkBar(a),this._currentItem=e}}},ON=(()=>{class t{_elementRef=m(L);_inkBarElement=null;_inkBarContentElement=null;_fitToContent=!1;get fitInkBarToContent(){return this._fitToContent}set fitInkBarToContent(e){this._fitToContent!==e&&(this._fitToContent=e,this._inkBarElement&&this._appendInkBarElement())}activateInkBar(e){let i=this._elementRef.nativeElement;if(!e||!i.getBoundingClientRect||!this._inkBarContentElement){i.classList.add(Zp);return}let a=i.getBoundingClientRect(),r=e.width/a.width,o=e.left-a.left;i.classList.add(cx),this._inkBarContentElement.style.setProperty("transform",`translateX(${o}px) scaleX(${r})`),i.getBoundingClientRect(),i.classList.remove(cx),i.classList.add(Zp),this._inkBarContentElement.style.setProperty("transform","")}deactivateInkBar(){this._elementRef.nativeElement.classList.remove(Zp)}ngOnInit(){this._createInkBarElement()}ngOnDestroy(){this._inkBarElement?.remove(),this._inkBarElement=this._inkBarContentElement=null}_createInkBarElement(){let e=this._elementRef.nativeElement.ownerDocument||document,i=this._inkBarElement=e.createElement("span"),a=this._inkBarContentElement=e.createElement("span");i.className="mdc-tab-indicator",a.className="mdc-tab-indicator__content mdc-tab-indicator__content--underline",i.appendChild(this._inkBarContentElement),this._appendInkBarElement()}_appendInkBarElement(){this._inkBarElement;let e=this._fitToContent?this._elementRef.nativeElement.querySelector(".mdc-tab__content"):this._elementRef.nativeElement;e.appendChild(this._inkBarElement)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,inputs:{fitInkBarToContent:[2,"fitInkBarToContent","fitInkBarToContent",I]}})}return t})();var px=(()=>{class t extends ON{elementRef=m(L);disabled=!1;focus(){this.elementRef.nativeElement.focus()}getOffsetLeft(){return this.elementRef.nativeElement.offsetLeft}getOffsetWidth(){return this.elementRef.nativeElement.offsetWidth}static \u0275fac=(()=>{let e;return function(a){return(e||(e=Fe(t)))(a||t)}})();static \u0275dir=D({type:t,selectors:[["","matTabLabelWrapper",""]],hostVars:3,hostBindings:function(i,a){i&2&&(K("aria-disabled",!!a.disabled),S("mat-mdc-tab-disabled",a.disabled))},inputs:{disabled:[2,"disabled","disabled",I]},features:[le]})}return t})(),lx={passive:!0},FN=650,LN=100;function Kp(t){let n=t+"";return/^[0-9]+(?:\.[0-9]+)?$/.test(n)?`${t}ms`:/^[0-9]+(?:\.[0-9]+)?(?:ms|s)$/.test(n)?n:""}var PN=(()=>{class t{_elementRef=m(L);_changeDetectorRef=m(Ie);_viewportRuler=m(na);_dir=m(Mt,{optional:!0});_ngZone=m(N);_platform=m(ye);_sharedResizeObserver=m(rl);_injector=m(X);_renderer=m(Se);_animationsDisabled=Re();_eventCleanups;_scrollDistance=0;_selectedIndexChanged=!1;_destroyed=new E;_showPaginationControls=!1;_disableScrollAfter=!0;_disableScrollBefore=!0;_tabLabelCount;_scrollDistanceChanged=!1;_keyManager;_currentTextContent;_stopScrolling=new E;disablePagination=!1;get selectedIndex(){return this._selectedIndex}set selectedIndex(e){let i=isNaN(e)?0:e;this._selectedIndex!=i&&(this._selectedIndexChanged=!0,this._selectedIndex=i,this._keyManager&&this._keyManager.updateActiveItem(i))}_selectedIndex=0;selectFocusedIndex=new z;indexFocused=new z;constructor(){this._eventCleanups=this._ngZone.runOutsideAngular(()=>[this._renderer.listen(this._elementRef.nativeElement,"mouseleave",()=>this._stopInterval())])}ngAfterViewInit(){this._eventCleanups.push(this._renderer.listen(this._previousPaginator.nativeElement,"touchstart",()=>this._handlePaginatorPress("before"),lx),this._renderer.listen(this._nextPaginator.nativeElement,"touchstart",()=>this._handlePaginatorPress("after"),lx))}ngAfterContentInit(){let e=this._dir?this._dir.change:In("ltr"),i=this._sharedResizeObserver.observe(this._elementRef.nativeElement).pipe(Nn(32),Oe(this._destroyed)),a=this._viewportRuler.change(150).pipe(Oe(this._destroyed)),r=()=>{this.updatePagination(),this._alignInkBarToSelectedTab()};this._keyManager=new Ji(this._items).withHorizontalOrientation(this._getLayoutDirection()).withHomeAndEnd().withWrap().skipPredicate(()=>!1),this._keyManager.updateActiveItem(Math.max(this._selectedIndex,0)),mt(r,{injector:this._injector}),jt(e,a,i,this._items.changes,this._itemsResized()).pipe(Oe(this._destroyed)).subscribe(()=>{this._ngZone.run(()=>{Promise.resolve().then(()=>{this._scrollDistance=Math.max(0,Math.min(this._getMaxScrollDistance(),this._scrollDistance)),r()})}),this._keyManager?.withHorizontalOrientation(this._getLayoutDirection())}),this._keyManager.change.subscribe(o=>{this.indexFocused.emit(o),this._setTabFocus(o)})}_itemsResized(){return typeof ResizeObserver!="function"?Dn:this._items.changes.pipe(yt(this._items),pa(e=>new se(i=>this._ngZone.runOutsideAngular(()=>{let a=new ResizeObserver(r=>i.next(r));return e.forEach(r=>a.observe(r.elementRef.nativeElement)),()=>{a.disconnect()}}))),xi(1),ot(e=>e.some(i=>i.contentRect.width>0&&i.contentRect.height>0)))}ngAfterContentChecked(){this._tabLabelCount!=this._items.length&&(this.updatePagination(),this._tabLabelCount=this._items.length,this._changeDetectorRef.markForCheck()),this._selectedIndexChanged&&(this._scrollToLabel(this._selectedIndex),this._checkScrollingControls(),this._alignInkBarToSelectedTab(),this._selectedIndexChanged=!1,this._changeDetectorRef.markForCheck()),this._scrollDistanceChanged&&(this._updateTabScrollPosition(),this._scrollDistanceChanged=!1,this._changeDetectorRef.markForCheck())}ngOnDestroy(){this._eventCleanups.forEach(e=>e()),this._keyManager?.destroy(),this._destroyed.next(),this._destroyed.complete(),this._stopScrolling.complete()}_handleKeydown(e){if(!$a(e))switch(e.keyCode){case 13:case 32:if(this.focusIndex!==this.selectedIndex){let i=this._items.get(this.focusIndex);i&&!i.disabled&&(this.selectFocusedIndex.emit(this.focusIndex),this._itemSelected(e))}break;default:this._keyManager?.onKeydown(e)}}_onContentChanges(){let e=this._elementRef.nativeElement.textContent;e!==this._currentTextContent&&(this._currentTextContent=e||"",this._ngZone.run(()=>{this.updatePagination(),this._alignInkBarToSelectedTab(),this._changeDetectorRef.markForCheck()}))}updatePagination(){this._checkPaginationEnabled(),this._checkScrollingControls(),this._updateTabScrollPosition()}get focusIndex(){return this._keyManager?this._keyManager.activeItemIndex:0}set focusIndex(e){!this._isValidIndex(e)||this.focusIndex===e||!this._keyManager||this._keyManager.setActiveItem(e)}_isValidIndex(e){return this._items?!!this._items.toArray()[e]:!0}_setTabFocus(e){if(this._showPaginationControls&&this._scrollToLabel(e),this._items&&this._items.length){this._items.toArray()[e].focus();let i=this._tabListContainer.nativeElement;this._getLayoutDirection()=="ltr"?i.scrollLeft=0:i.scrollLeft=i.scrollWidth-i.offsetWidth}}_getLayoutDirection(){return this._dir&&this._dir.value==="rtl"?"rtl":"ltr"}_updateTabScrollPosition(){if(this.disablePagination)return;let e=this.scrollDistance,i=this._getLayoutDirection()==="ltr"?-e:e;this._tabList.nativeElement.style.transform=`translateX(${Math.round(i)}px)`,(this._platform.TRIDENT||this._platform.EDGE)&&(this._tabListContainer.nativeElement.scrollLeft=0)}get scrollDistance(){return this._scrollDistance}set scrollDistance(e){this._scrollTo(e)}_scrollHeader(e){let i=this._tabListContainer.nativeElement.offsetWidth,a=(e=="before"?-1:1)*i/3;return this._scrollTo(this._scrollDistance+a)}_handlePaginatorClick(e){this._stopInterval(),this._scrollHeader(e)}_scrollToLabel(e){if(this.disablePagination)return;let i=this._items?this._items.toArray()[e]:null;if(!i)return;let a=this._tabListContainer.nativeElement.offsetWidth,{offsetLeft:r,offsetWidth:o}=i.elementRef.nativeElement,s,c;this._getLayoutDirection()=="ltr"?(s=r,c=s+o):(c=this._tabListInner.nativeElement.offsetWidth-r,s=c-o);let l=this.scrollDistance,d=this.scrollDistance+a;s<l?this.scrollDistance-=l-s:c>d&&(this.scrollDistance+=Math.min(c-d,s-l))}_checkPaginationEnabled(){if(this.disablePagination)this._showPaginationControls=!1;else{let e=this._tabListInner.nativeElement.scrollWidth,i=this._elementRef.nativeElement.offsetWidth,a=e-i>=5;a||(this.scrollDistance=0),a!==this._showPaginationControls&&(this._showPaginationControls=a,this._changeDetectorRef.markForCheck())}}_checkScrollingControls(){this.disablePagination?this._disableScrollAfter=this._disableScrollBefore=!0:(this._disableScrollBefore=this.scrollDistance==0,this._disableScrollAfter=this.scrollDistance==this._getMaxScrollDistance(),this._changeDetectorRef.markForCheck())}_getMaxScrollDistance(){let e=this._tabListInner.nativeElement.scrollWidth,i=this._tabListContainer.nativeElement.offsetWidth;return e-i||0}_alignInkBarToSelectedTab(){let e=this._items&&this._items.length?this._items.toArray()[this.selectedIndex]:null,i=e?e.elementRef.nativeElement:null;i?this._inkBar.alignToElement(i):this._inkBar.hide()}_stopInterval(){this._stopScrolling.next()}_handlePaginatorPress(e,i){i&&i.button!=null&&i.button!==0||(this._stopInterval(),or(FN,LN).pipe(Oe(jt(this._stopScrolling,this._destroyed))).subscribe(()=>{let{maxScrollDistance:a,distance:r}=this._scrollHeader(e);(r===0||r>=a)&&this._stopInterval()}))}_scrollTo(e){if(this.disablePagination)return{maxScrollDistance:0,distance:0};let i=this._getMaxScrollDistance();return this._scrollDistance=Math.max(0,Math.min(i,e)),this._scrollDistanceChanged=!0,this._checkScrollingControls(),{maxScrollDistance:i,distance:this._scrollDistance}}static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,inputs:{disablePagination:[2,"disablePagination","disablePagination",I],selectedIndex:[2,"selectedIndex","selectedIndex",ht]},outputs:{selectFocusedIndex:"selectFocusedIndex",indexFocused:"indexFocused"}})}return t})(),VN=(()=>{class t extends PN{_items;_tabListContainer;_tabList;_tabListInner;_nextPaginator;_previousPaginator;_inkBar;ariaLabel;ariaLabelledby;disableRipple=!1;ngAfterContentInit(){this._inkBar=new Qp(this._items),super.ngAfterContentInit()}_itemSelected(e){e.preventDefault()}static \u0275fac=(()=>{let e;return function(a){return(e||(e=Fe(t)))(a||t)}})();static \u0275cmp=(function(){let e=["tabListContainer"],i=["tabList"],a=["tabListInner"],r=["nextPaginator"],o=["previousPaginator"];return B({type:t,selectors:[["mat-tab-header"]],contentQueries:function(l,d,u){if(l&1&&Ye(u,px,4),l&2){let g;M(g=T())&&(d._items=g)}},viewQuery:function(l,d){if(l&1&&Be(e,7)(i,7)(a,7)(r,5)(o,5),l&2){let u;M(u=T())&&(d._tabListContainer=u.first),M(u=T())&&(d._tabList=u.first),M(u=T())&&(d._tabListInner=u.first),M(u=T())&&(d._nextPaginator=u.first),M(u=T())&&(d._previousPaginator=u.first)}},hostAttrs:[1,"mat-mdc-tab-header"],hostVars:4,hostBindings:function(l,d){l&2&&S("mat-mdc-tab-header-pagination-controls-enabled",d._showPaginationControls)("mat-mdc-tab-header-rtl",d._getLayoutDirection()=="rtl")},inputs:{ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],disableRipple:[2,"disableRipple","disableRipple",I]},features:[le],ngContentSelectors:["*"],decls:13,vars:10,consts:[["previousPaginator",""],["tabListContainer",""],["tabList",""],["tabListInner",""],["nextPaginator",""],["mat-ripple","",1,"mat-mdc-tab-header-pagination","mat-mdc-tab-header-pagination-before",3,"click","mousedown","touchend","matRippleDisabled"],[1,"mat-mdc-tab-header-pagination-chevron"],[1,"mat-mdc-tab-label-container",3,"keydown"],["role","tablist",1,"mat-mdc-tab-list",3,"cdkObserveContent"],[1,"mat-mdc-tab-labels"],["mat-ripple","",1,"mat-mdc-tab-header-pagination","mat-mdc-tab-header-pagination-after",3,"mousedown","click","touchend","matRippleDisabled"]],template:function(l,d){l&1&&(he(),p(0,"div",5,0),C("click",function(){return d._handlePaginatorClick("before")})("mousedown",function(g){return d._handlePaginatorPress("before",g)})("touchend",function(){return d._stopInterval()}),$(2,"div",6),h(),p(3,"div",7,1),C("keydown",function(g){return d._handleKeydown(g)}),p(5,"div",8,2),C("cdkObserveContent",function(){return d._onContentChanges()}),p(7,"div",9,3),V(9),h()()(),p(10,"div",10,4),C("mousedown",function(g){return d._handlePaginatorPress("after",g)})("click",function(){return d._handlePaginatorClick("after")})("touchend",function(){return d._stopInterval()}),$(12,"div",6),h()),l&2&&(S("mat-mdc-tab-header-pagination-disabled",d._disableScrollBefore),F("matRippleDisabled",d._disableScrollBefore||d.disableRipple),v(3),S("_mat-animation-noopable",d._animationsDisabled),v(2),K("aria-label",d.ariaLabel||null)("aria-labelledby",d.ariaLabelledby||null),v(5),S("mat-mdc-tab-header-pagination-disabled",d._disableScrollAfter),F("matRippleDisabled",d._disableScrollAfter||d.disableRipple))},dependencies:[Gn,Gc],styles:[`.mat-mdc-tab-header {
  display: flex;
  overflow: hidden;
  position: relative;
  flex-shrink: 0;
}

.mdc-tab-indicator .mdc-tab-indicator__content {
  transition-duration: var(--%NS%mat-tab-header-animation-duration, 250ms);
}

.mat-mdc-tab-header-pagination {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: none;
  justify-content: center;
  align-items: center;
  min-width: 32px;
  cursor: pointer;
  z-index: 2;
  -webkit-tap-highlight-color: transparent;
  touch-action: none;
  box-sizing: content-box;
  outline: 0;
}
.mat-mdc-tab-header-pagination::-moz-focus-inner {
  border: 0;
}
.mat-mdc-tab-header-pagination .mat-ripple-element {
  opacity: 0.12;
  background-color: var(--%NS%mat-tab-inactive-ripple-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-tab-header-pagination-controls-enabled .mat-mdc-tab-header-pagination {
  display: flex;
}

.mat-mdc-tab-header-pagination-before,
.mat-mdc-tab-header-rtl .mat-mdc-tab-header-pagination-after {
  padding-left: 4px;
}
.mat-mdc-tab-header-pagination-before .mat-mdc-tab-header-pagination-chevron,
.mat-mdc-tab-header-rtl .mat-mdc-tab-header-pagination-after .mat-mdc-tab-header-pagination-chevron {
  transform: rotate(-135deg);
}

.mat-mdc-tab-header-rtl .mat-mdc-tab-header-pagination-before,
.mat-mdc-tab-header-pagination-after {
  padding-right: 4px;
}
.mat-mdc-tab-header-rtl .mat-mdc-tab-header-pagination-before .mat-mdc-tab-header-pagination-chevron,
.mat-mdc-tab-header-pagination-after .mat-mdc-tab-header-pagination-chevron {
  transform: rotate(45deg);
}

.mat-mdc-tab-header-pagination-chevron {
  border-style: solid;
  border-width: 2px 2px 0 0;
  height: 8px;
  width: 8px;
  border-color: var(--%NS%mat-tab-pagination-icon-color, var(--%NS%mat-sys-on-surface));
}

.mat-mdc-tab-header-pagination-disabled {
  box-shadow: none;
  cursor: default;
  pointer-events: none;
}
.mat-mdc-tab-header-pagination-disabled .mat-mdc-tab-header-pagination-chevron {
  opacity: 0.4;
}

.mat-mdc-tab-list {
  flex-grow: 1;
  position: relative;
  transition: transform 500ms cubic-bezier(0.35, 0, 0.25, 1);
}
._mat-animation-noopable .mat-mdc-tab-list {
  transition: none;
}

.mat-mdc-tab-label-container {
  display: flex;
  flex-grow: 1;
  overflow: hidden;
  z-index: 1;
  border-bottom-style: solid;
  border-bottom-width: var(--%NS%mat-tab-divider-height, 1px);
  border-bottom-color: var(--%NS%mat-tab-divider-color, var(--%NS%mat-sys-surface-variant));
}
.mat-mdc-tab-group-inverted-header .mat-mdc-tab-label-container {
  border-bottom: none;
  border-top-style: solid;
  border-top-width: var(--%NS%mat-tab-divider-height, 1px);
  border-top-color: var(--%NS%mat-tab-divider-color, var(--%NS%mat-sys-surface-variant));
}

.mat-mdc-tab-labels {
  display: flex;
  flex: 1 0 auto;
}
[mat-align-tabs=center] > .mat-mdc-tab-header .mat-mdc-tab-labels {
  justify-content: center;
}
[mat-align-tabs=end] > .mat-mdc-tab-header .mat-mdc-tab-labels {
  justify-content: flex-end;
}
.cdk-drop-list .mat-mdc-tab-labels, .mat-mdc-tab-labels.cdk-drop-list {
  min-height: var(--%NS%mat-tab-container-height, 48px);
}

.mat-mdc-tab::before {
  margin: 5px;
}
@media (forced-colors: active) {
  .mat-mdc-tab[aria-disabled=true] {
    color: GrayText;
  }
}
`],encapsulation:2,changeDetection:1})})()}return t})(),BN=new y("MAT_TABS_CONFIG"),dx=(()=>{class t extends Ka{_host=m(Xp);_ngZone=m(N);_centeringSub=pe.EMPTY;_leavingSub=pe.EMPTY;ngOnInit(){super.ngOnInit(),this._centeringSub=this._host._beforeCentering.pipe(yt(this._host._isCenterPosition())).subscribe(e=>{this._host._content&&e&&!this.hasAttached()&&this._ngZone.run(()=>{Promise.resolve().then(),this.attach(this._host._content)})}),this._leavingSub=this._host._afterLeavingCenter.subscribe(()=>{this._host.preserveContent||this._ngZone.run(()=>this.detach())})}ngOnDestroy(){super.ngOnDestroy(),this._centeringSub.unsubscribe(),this._leavingSub.unsubscribe()}static \u0275fac=(()=>{let e;return function(a){return(e||(e=Fe(t)))(a||t)}})();static \u0275dir=D({type:t,selectors:[["","matTabBodyHost",""]],features:[le]})}return t})(),Xp=(()=>{class t{_elementRef=m(L);_dir=m(Mt,{optional:!0});_ngZone=m(N);_injector=m(X);_renderer=m(Se);_diAnimationsDisabled=Re();_eventCleanups;_initialized=!1;_fallbackTimer;_positionIndex;_dirChangeSubscription=pe.EMPTY;_position;_previousPosition;_onCentering=new z;_beforeCentering=new z;_afterLeavingCenter=new z;_onCentered=new z(!0);_portalHost;_contentElement;_content;animationDuration="500ms";preserveContent=!1;set position(e){this._positionIndex=e,this._computePositionAnimationState()}constructor(){if(this._dir){let e=m(Ie);this._dirChangeSubscription=this._dir.change.subscribe(i=>{this._computePositionAnimationState(i),e.markForCheck()})}}ngOnInit(){this._bindTransitionEvents(),this._position==="center"&&(this._setActiveClass(!0),mt(()=>this._onCentering.emit(this._elementRef.nativeElement.clientHeight),{injector:this._injector})),this._initialized=!0}ngOnDestroy(){clearTimeout(this._fallbackTimer),this._eventCleanups?.forEach(e=>e()),this._dirChangeSubscription.unsubscribe()}_bindTransitionEvents(){this._ngZone.runOutsideAngular(()=>{let e=this._elementRef.nativeElement,i=a=>{a.target===this._contentElement?.nativeElement&&(this._elementRef.nativeElement.classList.remove("mat-tab-body-animating"),a.type==="transitionend"&&this._transitionDone())};this._eventCleanups=[this._renderer.listen(e,"transitionstart",a=>{a.target===this._contentElement?.nativeElement&&(this._elementRef.nativeElement.classList.add("mat-tab-body-animating"),this._transitionStarted())}),this._renderer.listen(e,"transitionend",i),this._renderer.listen(e,"transitioncancel",i)]})}_transitionStarted(){clearTimeout(this._fallbackTimer);let e=this._position==="center";this._beforeCentering.emit(e),e&&this._onCentering.emit(this._elementRef.nativeElement.clientHeight)}_transitionDone(){this._position==="center"?this._onCentered.emit():this._previousPosition==="center"&&this._afterLeavingCenter.emit()}_setActiveClass(e){this._elementRef.nativeElement.classList.toggle("mat-mdc-tab-body-active",e)}_getLayoutDirection(){return this._dir&&this._dir.value==="rtl"?"rtl":"ltr"}_isCenterPosition(){return this._positionIndex===0}_computePositionAnimationState(e=this._getLayoutDirection()){this._previousPosition=this._position,this._positionIndex<0?this._position=e=="ltr"?"left":"right":this._positionIndex>0?this._position=e=="ltr"?"right":"left":this._position="center",this._animationsDisabled()?this._simulateTransitionEvents():this._initialized&&(this._position==="center"||this._previousPosition==="center")&&(clearTimeout(this._fallbackTimer),this._fallbackTimer=this._ngZone.runOutsideAngular(()=>setTimeout(()=>this._simulateTransitionEvents(),100)))}_simulateTransitionEvents(){this._transitionStarted(),mt(()=>this._transitionDone(),{injector:this._injector})}_animationsDisabled(){return this._diAnimationsDisabled||this.animationDuration==="0ms"||this.animationDuration==="0s"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){let e=["content"];function i(a,r){}return B({type:t,selectors:[["mat-tab-body"]],viewQuery:function(r,o){if(r&1&&Be(dx,5)(e,5),r&2){let s;M(s=T())&&(o._portalHost=s.first),M(s=T())&&(o._contentElement=s.first)}},hostAttrs:[1,"mat-mdc-tab-body"],hostVars:1,hostBindings:function(r,o){r&2&&K("inert",o._position==="center"?null:"")},inputs:{_content:[0,"content","_content"],animationDuration:"animationDuration",preserveContent:"preserveContent",position:"position"},outputs:{_onCentering:"_onCentering",_beforeCentering:"_beforeCentering",_onCentered:"_onCentered"},decls:3,vars:6,consts:[["content",""],["cdkScrollable","",1,"mat-mdc-tab-body-content"],["matTabBodyHost",""]],template:function(r,o){r&1&&(p(0,"div",1,0),It(2,i,0,0,"ng-template",2),h()),r&2&&S("mat-tab-body-content-left",o._position==="left")("mat-tab-body-content-right",o._position==="right")("mat-tab-body-content-can-animate",o._position==="center"||o._previousPosition==="center")},dependencies:[dx,sx],styles:[`.mat-mdc-tab-body {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  display: block;
  overflow: hidden;
  outline: 0;
  flex-basis: 100%;
}
.mat-mdc-tab-body.mat-mdc-tab-body-active {
  position: relative;
  overflow-x: hidden;
  overflow-y: auto;
  z-index: 1;
  flex-grow: 1;
}
.mat-mdc-tab-group.mat-mdc-tab-group-dynamic-height .mat-mdc-tab-body.mat-mdc-tab-body-active {
  overflow-y: hidden;
}

.mat-mdc-tab-body-content {
  height: 100%;
  overflow: auto;
  transform: none;
  visibility: hidden;
}
.mat-tab-body-animating > .mat-mdc-tab-body-content, .mat-mdc-tab-body-active > .mat-mdc-tab-body-content {
  visibility: visible;
}
.mat-tab-body-animating > .mat-mdc-tab-body-content {
  min-height: 1px;
}
.mat-mdc-tab-group-dynamic-height .mat-mdc-tab-body-content {
  overflow: hidden;
}

.mat-tab-body-content-can-animate {
  transition: transform var(--%NS%mat-tab-body-animation-duration) 1ms cubic-bezier(0.35, 0, 0.25, 1);
}
.mat-mdc-tab-body-wrapper._mat-animation-noopable .mat-tab-body-content-can-animate {
  transition: none;
}

.mat-tab-body-content-left {
  transform: translate3d(-100%, 0, 0);
}

.mat-tab-body-content-right {
  transform: translate3d(100%, 0, 0);
}
`],encapsulation:2,changeDetection:1})})()}return t})(),hx=(()=>{class t{_elementRef=m(L);_changeDetectorRef=m(Ie);_ngZone=m(N);_tabsSubscription=pe.EMPTY;_tabLabelSubscription=pe.EMPTY;_tabBodySubscription=pe.EMPTY;_diAnimationsDisabled=Re();_bodyAnimationDuration;_headerAnimationDuration;_allTabs;_tabBodies;_tabBodyWrapper;_tabHeader;_tabs=new nn;_indexToSelect=0;_lastFocusedTabIndex=null;_tabBodyWrapperHeight=0;color;get fitInkBarToContent(){return this._fitInkBarToContent}set fitInkBarToContent(e){this._fitInkBarToContent=e,this._changeDetectorRef.markForCheck()}_fitInkBarToContent=!1;stretchTabs=!0;alignTabs=null;dynamicHeight=!1;get selectedIndex(){return this._selectedIndex}set selectedIndex(e){this._indexToSelect=isNaN(e)?null:e}_selectedIndex=null;headerPosition="above";get animationDuration(){return this._animationDuration}set animationDuration(e){this._animationDuration=e,e&&typeof e=="object"?(this._bodyAnimationDuration=Kp(e.body),this._headerAnimationDuration=Kp(e.header)):this._headerAnimationDuration=this._bodyAnimationDuration=Kp(e)}_animationDuration;get contentTabIndex(){return this._contentTabIndex}set contentTabIndex(e){this._contentTabIndex=isNaN(e)?null:e}_contentTabIndex=null;disablePagination=!1;disableRipple=!1;preserveContent=!1;get backgroundColor(){return this._backgroundColor}set backgroundColor(e){let i=this._elementRef.nativeElement.classList;i.remove("mat-tabs-with-background",`mat-background-${this.backgroundColor}`),e&&i.add("mat-tabs-with-background",`mat-background-${e}`),this._backgroundColor=e}_backgroundColor;ariaLabel;ariaLabelledby;selectedIndexChange=new z;focusChange=new z;animationDone=new z;selectedTabChange=new z(!0);_groupId;_isServer=!m(ye).isBrowser;constructor(){let e=m(BN,{optional:!0});this._groupId=m(Pe).getId("mat-tab-group-"),this.animationDuration=e&&e.animationDuration?e.animationDuration:"500ms",this.disablePagination=e&&e.disablePagination!=null?e.disablePagination:!1,this.dynamicHeight=e&&e.dynamicHeight!=null?e.dynamicHeight:!1,e?.contentTabIndex!=null&&(this.contentTabIndex=e.contentTabIndex),this.preserveContent=!!e?.preserveContent,this.fitInkBarToContent=e&&e.fitInkBarToContent!=null?e.fitInkBarToContent:!1,this.stretchTabs=e&&e.stretchTabs!=null?e.stretchTabs:!0,this.alignTabs=e&&e.alignTabs!=null?e.alignTabs:null}ngAfterContentChecked(){let e=this._indexToSelect=this._clampTabIndex(this._indexToSelect);if(this._selectedIndex!=e){let i=this._selectedIndex==null;if(!i){this.selectedTabChange.emit(this._createChangeEvent(e));let a=this._tabBodyWrapper.nativeElement;a.style.minHeight=a.clientHeight+"px"}Promise.resolve().then(()=>{this._tabs.forEach((a,r)=>a.isActive=r===e),i||(this.selectedIndexChange.emit(e),this._tabBodyWrapper.nativeElement.style.minHeight="")})}this._tabs.forEach((i,a)=>{i.position=a-e,this._selectedIndex!=null&&i.position==0&&!i.origin&&(i.origin=e-this._selectedIndex)}),this._selectedIndex!==e&&(this._selectedIndex=e,this._lastFocusedTabIndex=null,this._changeDetectorRef.markForCheck())}ngAfterContentInit(){this._subscribeToAllTabChanges(),this._subscribeToTabLabels(),this._tabsSubscription=this._tabs.changes.subscribe(()=>{let e=this._clampTabIndex(this._indexToSelect);if(e===this._selectedIndex){let i=this._tabs.toArray(),a;for(let r=0;r<i.length;r++)if(i[r].isActive){this._indexToSelect=this._selectedIndex=r,this._lastFocusedTabIndex=null,a=i[r];break}!a&&i[e]&&Promise.resolve().then(()=>{i[e].isActive=!0,this.selectedTabChange.emit(this._createChangeEvent(e))})}this._changeDetectorRef.markForCheck()})}ngAfterViewInit(){this._tabBodySubscription=this._tabBodies.changes.subscribe(()=>this._bodyCentered(!0))}_subscribeToAllTabChanges(){this._allTabs.changes.pipe(yt(this._allTabs)).subscribe(e=>{this._tabs.reset(e.filter(i=>i._closestTabGroup===this||!i._closestTabGroup)),this._tabs.notifyOnChanges()})}ngOnDestroy(){this._tabs.destroy(),this._tabsSubscription.unsubscribe(),this._tabLabelSubscription.unsubscribe(),this._tabBodySubscription.unsubscribe()}realignInkBar(){this._tabHeader&&this._tabHeader._alignInkBarToSelectedTab()}updatePagination(){this._tabHeader&&this._tabHeader.updatePagination()}focusTab(e){let i=this._tabHeader;i&&(i.focusIndex=e)}_focusChanged(e){this._lastFocusedTabIndex=e,this.focusChange.emit(this._createChangeEvent(e))}_createChangeEvent(e){let i=new Jp;return i.index=e,this._tabs&&this._tabs.length&&(i.tab=this._tabs.toArray()[e]),i}_subscribeToTabLabels(){this._tabLabelSubscription&&this._tabLabelSubscription.unsubscribe(),this._tabLabelSubscription=jt(...this._tabs.map(e=>e._stateChanges)).subscribe(()=>this._changeDetectorRef.markForCheck())}_clampTabIndex(e){return Math.min(this._tabs.length-1,Math.max(e||0,0))}_getTabLabelId(e,i){return e.id||`${this._groupId}-label-${i}`}_getTabContentId(e){return`${this._groupId}-content-${e}`}_setTabBodyWrapperHeight(e){if(!this.dynamicHeight||!this._tabBodyWrapperHeight){this._tabBodyWrapperHeight=e;return}let i=this._tabBodyWrapper.nativeElement;i.style.height=this._tabBodyWrapperHeight+"px",this._tabBodyWrapper.nativeElement.offsetHeight&&(i.style.height=e+"px")}_removeTabBodyWrapperHeight(){let e=this._tabBodyWrapper.nativeElement;this._tabBodyWrapperHeight=e.clientHeight,e.style.height="",this._ngZone.run(()=>this.animationDone.emit())}_handleClick(e,i,a){i.focusIndex=a,e.disabled||(this.selectedIndex=a)}_getTabIndex(e){let i=this._lastFocusedTabIndex??this.selectedIndex;return e===i?0:-1}_tabFocusChanged(e,i){e&&e!=="mouse"&&e!=="touch"&&(this._tabHeader.focusIndex=i)}_bodyCentered(e){e&&this._tabBodies?.forEach((i,a)=>i._setActiveClass(a===this._selectedIndex))}_bodyAnimationsDisabled(){return this._diAnimationsDisabled||this._bodyAnimationDuration==="0"||this._bodyAnimationDuration==="0ms"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){let e=["tabBodyWrapper"],i=["tabHeader"],a=["*"];function r(u,g){}function o(u,g){if(u&1&&It(0,r,0,0,"ng-template",12),u&2){let f=x().$implicit;F("cdkPortalOutlet",f.templateLabel)}}function s(u,g){if(u&1&&b(0),u&2){let f=x().$implicit;de(f.textLabel)}}function c(u,g){if(u&1){let f=ve();p(0,"div",7,2),C("click",function(){let w=R(f),A=w.$implicit,ie=w.$index,fe=x(),cn=et(1);return O(fe._handleClick(A,cn,ie))})("cdkFocusChange",function(w){let A=R(f).$index,ie=x();return O(ie._tabFocusChanged(w,A))}),$(2,"span",8)(3,"div",9),p(4,"span",10)(5,"span",11),te(6,o,1,1,null,12)(7,s,1,1),h()()()}if(u&2){let f=g.$implicit,_=g.$index,w=et(1),A=x();xt(f.labelClass),S("mdc-tab--active",A.selectedIndex===_),F("id",A._getTabLabelId(f,_))("disabled",f.disabled)("fitInkBarToContent",A.fitInkBarToContent),K("tabIndex",A._getTabIndex(_))("aria-posinset",_+1)("aria-setsize",A._tabs.length)("aria-controls",A._getTabContentId(_))("aria-selected",A.selectedIndex===_)("aria-label",f.ariaLabel||null)("aria-labelledby",!f.ariaLabel&&f.ariaLabelledby?f.ariaLabelledby:null),v(3),F("matRippleTrigger",w)("matRippleDisabled",f.disabled||A.disableRipple),v(3),ne(f.templateLabel?6:7)}}function l(u,g){u&1&&V(0)}function d(u,g){if(u&1){let f=ve();p(0,"mat-tab-body",13),C("_onCentered",function(){R(f);let w=x();return O(w._removeTabBodyWrapperHeight())})("_onCentering",function(w){R(f);let A=x();return O(A._setTabBodyWrapperHeight(w))})("_beforeCentering",function(w){R(f);let A=x();return O(A._bodyCentered(w))}),h()}if(u&2){let f=g.$implicit,_=g.$index,w=x();xt(f.bodyClass),F("id",w._getTabContentId(_))("content",f.content)("position",f.position)("animationDuration",w._bodyAnimationDuration)("preserveContent",w.preserveContent),K("tabindex",w.contentTabIndex!=null&&w.selectedIndex===_?w.contentTabIndex:null)("aria-labelledby",w._getTabLabelId(f,_))("aria-hidden",w.selectedIndex!==_)}}return B({type:t,selectors:[["mat-tab-group"]],contentQueries:function(g,f,_){if(g&1&&Ye(_,eh,5),g&2){let w;M(w=T())&&(f._allTabs=w)}},viewQuery:function(g,f){if(g&1&&Be(e,5)(i,5)(Xp,5),g&2){let _;M(_=T())&&(f._tabBodyWrapper=_.first),M(_=T())&&(f._tabHeader=_.first),M(_=T())&&(f._tabBodies=_)}},hostAttrs:[1,"mat-mdc-tab-group"],hostVars:13,hostBindings:function(g,f){g&2&&(K("mat-align-tabs",f.alignTabs),xt("mat-"+(f.color||"primary")),oi("--%NS%mat-tab-body-animation-duration",f._bodyAnimationDuration)("--%NS%mat-tab-header-animation-duration",f._headerAnimationDuration),S("mat-mdc-tab-group-dynamic-height",f.dynamicHeight)("mat-mdc-tab-group-inverted-header",f.headerPosition==="below")("mat-mdc-tab-group-stretch-tabs",f.stretchTabs))},inputs:{color:"color",fitInkBarToContent:[2,"fitInkBarToContent","fitInkBarToContent",I],stretchTabs:[2,"mat-stretch-tabs","stretchTabs",I],alignTabs:[0,"mat-align-tabs","alignTabs"],dynamicHeight:[2,"dynamicHeight","dynamicHeight",I],selectedIndex:[2,"selectedIndex","selectedIndex",ht],headerPosition:"headerPosition",animationDuration:"animationDuration",contentTabIndex:[2,"contentTabIndex","contentTabIndex",ht],disablePagination:[2,"disablePagination","disablePagination",I],disableRipple:[2,"disableRipple","disableRipple",I],preserveContent:[2,"preserveContent","preserveContent",I],backgroundColor:"backgroundColor",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"]},outputs:{selectedIndexChange:"selectedIndexChange",focusChange:"focusChange",animationDone:"animationDone",selectedTabChange:"selectedTabChange"},exportAs:["matTabGroup"],features:[_e([{provide:ux,useExisting:t}])],ngContentSelectors:a,decls:9,vars:8,consts:[["tabHeader",""],["tabBodyWrapper",""],["tabNode",""],[3,"indexFocused","selectFocusedIndex","selectedIndex","disableRipple","disablePagination","aria-label","aria-labelledby"],["role","tab","matTabLabelWrapper","","cdkMonitorElementFocus","",1,"mdc-tab","mat-mdc-tab","mat-focus-indicator",3,"id","mdc-tab--active","class","disabled","fitInkBarToContent"],[1,"mat-mdc-tab-body-wrapper"],["role","tabpanel",3,"id","class","content","position","animationDuration","preserveContent"],["role","tab","matTabLabelWrapper","","cdkMonitorElementFocus","",1,"mdc-tab","mat-mdc-tab","mat-focus-indicator",3,"click","cdkFocusChange","id","disabled","fitInkBarToContent"],[1,"mdc-tab__ripple"],["mat-ripple","",1,"mat-mdc-tab-ripple",3,"matRippleTrigger","matRippleDisabled"],[1,"mdc-tab__content"],[1,"mdc-tab__text-label"],[3,"cdkPortalOutlet"],["role","tabpanel",3,"_onCentered","_onCentering","_beforeCentering","id","content","position","animationDuration","preserveContent"]],template:function(g,f){g&1&&(he(),p(0,"mat-tab-header",3,0),C("indexFocused",function(w){return f._focusChanged(w)})("selectFocusedIndex",function(w){return f.selectedIndex=w}),ut(2,c,8,17,"div",4,yn),h(),te(4,l,1,0),p(5,"div",5,1),ut(7,d,1,10,"mat-tab-body",6,yn),h()),g&2&&(F("selectedIndex",f.selectedIndex||0)("disableRipple",f.disableRipple)("disablePagination",f.disablePagination),lc("aria-label",f.ariaLabel)("aria-labelledby",f.ariaLabelledby),v(2),pt(f._tabs),v(2),ne(f._isServer?4:-1),v(),S("_mat-animation-noopable",f._bodyAnimationsDisabled()),v(2),pt(f._tabs))},dependencies:[VN,px,hp,Gn,Ka,Xp],styles:[`.mdc-tab {
  min-width: 90px;
  padding: 0 24px;
  display: flex;
  flex: 1 0 auto;
  justify-content: center;
  box-sizing: border-box;
  border: none;
  outline: none;
  text-align: center;
  white-space: nowrap;
  cursor: pointer;
  z-index: 1;
  touch-action: manipulation;
}

.mdc-tab__content {
  display: flex;
  align-items: center;
  justify-content: center;
  height: inherit;
  pointer-events: none;
}

.mdc-tab__text-label {
  transition: 150ms color linear;
  display: inline-block;
  line-height: 1;
  z-index: 2;
}

.mdc-tab--active .mdc-tab__text-label {
  transition-delay: 100ms;
}

._mat-animation-noopable .mdc-tab__text-label {
  transition: none;
}

.mdc-tab-indicator {
  display: flex;
  position: absolute;
  top: 0;
  left: 0;
  justify-content: center;
  width: 100%;
  height: 100%;
  pointer-events: none;
  z-index: 1;
}

.mdc-tab-indicator__content {
  transition: var(--%NS%mat-tab-header-animation-duration, 250ms) transform cubic-bezier(0.4, 0, 0.2, 1);
  transform-origin: left;
  opacity: 0;
}

.mdc-tab-indicator__content--underline {
  align-self: flex-end;
  box-sizing: border-box;
  width: 100%;
  border-top-style: solid;
}

.mdc-tab-indicator--active .mdc-tab-indicator__content {
  opacity: 1;
}

._mat-animation-noopable .mdc-tab-indicator__content, .mdc-tab-indicator--no-transition .mdc-tab-indicator__content {
  transition: none;
}

.mat-mdc-tab-ripple.mat-mdc-tab-ripple {
  position: absolute;
  top: 0;
  left: 0;
  bottom: 0;
  right: 0;
  pointer-events: none;
}

.mat-mdc-tab {
  -webkit-tap-highlight-color: transparent;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-decoration: none;
  background: none;
  height: var(--%NS%mat-tab-container-height, 48px);
  font-family: var(--%NS%mat-tab-label-text-font, var(--%NS%mat-sys-title-small-font));
  font-size: var(--%NS%mat-tab-label-text-size, var(--%NS%mat-sys-title-small-size));
  letter-spacing: var(--%NS%mat-tab-label-text-tracking, var(--%NS%mat-sys-title-small-tracking));
  line-height: var(--%NS%mat-tab-label-text-line-height, var(--%NS%mat-sys-title-small-line-height));
  font-weight: var(--%NS%mat-tab-label-text-weight, var(--%NS%mat-sys-title-small-weight));
}
.mat-mdc-tab.mdc-tab {
  flex-grow: 0;
}
.mat-mdc-tab .mdc-tab-indicator__content--underline {
  border-color: var(--%NS%mat-tab-active-indicator-color, var(--%NS%mat-sys-primary));
  border-top-width: var(--%NS%mat-tab-active-indicator-height, 2px);
  border-radius: var(--%NS%mat-tab-active-indicator-shape, 0);
}
.mat-mdc-tab:hover .mdc-tab__text-label {
  color: var(--%NS%mat-tab-inactive-hover-label-text-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-tab:focus .mdc-tab__text-label {
  color: var(--%NS%mat-tab-inactive-focus-label-text-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-tab.mdc-tab--active .mdc-tab__text-label {
  color: var(--%NS%mat-tab-active-label-text-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-tab.mdc-tab--active .mdc-tab__ripple::before,
.mat-mdc-tab.mdc-tab--active .mat-ripple-element {
  background-color: var(--%NS%mat-tab-active-ripple-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-tab.mdc-tab--%NS%active:hover .mdc-tab__text-label {
  color: var(--%NS%mat-tab-active-hover-label-text-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-tab.mdc-tab--%NS%active:hover .mdc-tab-indicator__content--underline {
  border-color: var(--%NS%mat-tab-active-hover-indicator-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-tab.mdc-tab--%NS%active:focus .mdc-tab__text-label {
  color: var(--%NS%mat-tab-active-focus-label-text-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-tab.mdc-tab--%NS%active:focus .mdc-tab-indicator__content--underline {
  border-color: var(--%NS%mat-tab-active-focus-indicator-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-tab.mat-mdc-tab-disabled {
  opacity: 0.4;
  pointer-events: none;
}
.mat-mdc-tab.mat-mdc-tab-disabled .mdc-tab__content {
  pointer-events: none;
}
.mat-mdc-tab.mat-mdc-tab-disabled .mdc-tab__ripple::before,
.mat-mdc-tab.mat-mdc-tab-disabled .mat-ripple-element {
  background-color: var(--%NS%mat-tab-disabled-ripple-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-tab .mdc-tab__ripple::before {
  content: "";
  display: block;
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  opacity: 0;
  pointer-events: none;
  background-color: var(--%NS%mat-tab-inactive-ripple-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-tab .mdc-tab__text-label {
  color: var(--%NS%mat-tab-inactive-label-text-color, var(--%NS%mat-sys-on-surface));
  display: inline-flex;
  align-items: center;
}
.mat-mdc-tab .mdc-tab__content {
  position: relative;
  pointer-events: auto;
}
.mat-mdc-tab:hover .mdc-tab__ripple::before {
  opacity: 0.04;
}
.mat-mdc-tab.cdk-program-focused .mdc-tab__ripple::before, .mat-mdc-tab.cdk-keyboard-focused .mdc-tab__ripple::before {
  opacity: 0.12;
}
.mat-mdc-tab .mat-ripple-element {
  opacity: 0.12;
  background-color: var(--%NS%mat-tab-inactive-ripple-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-tab-group.mat-mdc-tab-group-stretch-tabs > .mat-mdc-tab-header .mat-mdc-tab {
  flex-grow: 1;
}

.mat-mdc-tab-group {
  display: flex;
  flex-direction: column;
  max-width: 100%;
}
.mat-mdc-tab-group.mat-tabs-with-background > .mat-mdc-tab-header, .mat-mdc-tab-group.mat-tabs-with-background > .mat-mdc-tab-header-pagination {
  background-color: var(--%NS%mat-tab-background-color);
}
.mat-mdc-tab-group.mat-tabs-with-background.mat-primary > .mat-mdc-tab-header .mat-mdc-tab .mdc-tab__text-label {
  color: var(--%NS%mat-tab-foreground-color);
}
.mat-mdc-tab-group.mat-tabs-with-background.mat-primary > .mat-mdc-tab-header .mdc-tab-indicator__content--underline {
  border-color: var(--%NS%mat-tab-foreground-color);
}
.mat-mdc-tab-group.mat-tabs-with-background:not(.mat-primary) > .mat-mdc-tab-header .mat-mdc-tab:not(.mdc-tab--active) .mdc-tab__text-label {
  color: var(--%NS%mat-tab-foreground-color);
}
.mat-mdc-tab-group.mat-tabs-with-background:not(.mat-primary) > .mat-mdc-tab-header .mat-mdc-tab:not(.mdc-tab--active) .mdc-tab-indicator__content--underline {
  border-color: var(--%NS%mat-tab-foreground-color);
}
.mat-mdc-tab-group.mat-tabs-with-background > .mat-mdc-tab-header .mat-mdc-tab-header-pagination-chevron,
.mat-mdc-tab-group.mat-tabs-with-background > .mat-mdc-tab-header .mat-focus-indicator::before, .mat-mdc-tab-group.mat-tabs-with-background > .mat-mdc-tab-header-pagination .mat-mdc-tab-header-pagination-chevron,
.mat-mdc-tab-group.mat-tabs-with-background > .mat-mdc-tab-header-pagination .mat-focus-indicator::before {
  border-color: var(--%NS%mat-tab-foreground-color);
}
.mat-mdc-tab-group.mat-tabs-with-background > .mat-mdc-tab-header .mat-ripple-element, .mat-mdc-tab-group.mat-tabs-with-background > .mat-mdc-tab-header .mdc-tab__ripple::before, .mat-mdc-tab-group.mat-tabs-with-background > .mat-mdc-tab-header-pagination .mat-ripple-element, .mat-mdc-tab-group.mat-tabs-with-background > .mat-mdc-tab-header-pagination .mdc-tab__ripple::before {
  background-color: var(--%NS%mat-tab-foreground-color);
}
.mat-mdc-tab-group.mat-tabs-with-background > .mat-mdc-tab-header .mat-mdc-tab-header-pagination-chevron, .mat-mdc-tab-group.mat-tabs-with-background > .mat-mdc-tab-header-pagination .mat-mdc-tab-header-pagination-chevron {
  color: var(--%NS%mat-tab-foreground-color);
}
.mat-mdc-tab-group.mat-mdc-tab-group-inverted-header {
  flex-direction: column-reverse;
}
.mat-mdc-tab-group.mat-mdc-tab-group-inverted-header .mdc-tab-indicator__content--underline {
  align-self: flex-start;
}

.mat-mdc-tab-body-wrapper {
  position: relative;
  overflow: hidden;
  display: flex;
  transition: height 500ms cubic-bezier(0.35, 0, 0.25, 1);
}
.mat-mdc-tab-body-wrapper._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
`],encapsulation:2,changeDetection:1})})()}return t})(),Jp=class{index;tab};var fx=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=G({imports:[me]})}return t})();var gx=Kc();function kx(t){return new vl(t.get(na),t.get(ee))}var vl=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(n,e){this._viewportRuler=n,this._document=e}attach(){}enable(){if(this._canBeEnabled()){let n=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=n.style.left||"",this._previousHTMLStyles.top=n.style.top||"",n.style.left=Ze(-this._previousScrollPosition.left),n.style.top=Ze(-this._previousScrollPosition.top),n.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let n=this._document.documentElement,e=this._document.body,i=n.style,a=e.style,r=i.scrollBehavior||"",o=a.scrollBehavior||"";this._isEnabled=!1,i.left=this._previousHTMLStyles.left,i.top=this._previousHTMLStyles.top,n.classList.remove("cdk-global-scrollblock"),gx&&(i.scrollBehavior=a.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),gx&&(i.scrollBehavior=r,a.scrollBehavior=o)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let e=this._document.documentElement,i=this._viewportRuler.getViewportSize();return e.scrollHeight>i.height||e.scrollWidth>i.width}};function Cx(t,n){return new _l(t.get(bl),t.get(N),t.get(na),n)}var _l=class{_scrollDispatcher;_ngZone;_viewportRuler;_config;_scrollSubscription=null;_overlayRef;_initialScrollPosition;constructor(n,e,i,a){this._scrollDispatcher=n,this._ngZone=e,this._viewportRuler=i,this._config=a}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(this._scrollSubscription)return;let n=this._scrollDispatcher.scrolled(0).pipe(ot(e=>!e||!this._overlayRef.overlayElement.contains(e.getElementRef().nativeElement)));this._config&&this._config.threshold&&this._config.threshold>1?(this._initialScrollPosition=this._viewportRuler.getViewportScrollPosition().top,this._scrollSubscription=n.subscribe(()=>{let e=this._viewportRuler.getViewportScrollPosition().top;Math.abs(e-this._initialScrollPosition)>this._config.threshold?this._detach():this._overlayRef.updatePosition()})):this._scrollSubscription=n.subscribe(this._detach)}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}_detach=()=>{this.disable(),this._overlayRef.hasAttached()&&this._ngZone.run(()=>this._overlayRef.detach())}};var mo=class{enable(){}disable(){}attach(){}};function nh(t,n){return n.some(e=>{let i=t.bottom<e.top,a=t.top>e.bottom,r=t.right<e.left,o=t.left>e.right;return i||a||r||o})}function bx(t,n){return n.some(e=>{let i=t.top<e.top,a=t.bottom>e.bottom,r=t.left<e.left,o=t.right>e.right;return i||a||r||o})}function Sx(t,n){return new yl(t.get(bl),t.get(na),t.get(N),n)}var yl=class{_scrollDispatcher;_viewportRuler;_ngZone;_config;_scrollSubscription=null;_overlayRef;constructor(n,e,i,a){this._scrollDispatcher=n,this._viewportRuler=e,this._ngZone=i,this._config=a}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(!this._scrollSubscription){let n=this._config?this._config.scrollThrottle:0;this._scrollSubscription=this._scrollDispatcher.scrolled(n).subscribe(()=>{if(this._overlayRef.updatePosition(),this._config&&this._config.autoClose){let e=this._overlayRef.overlayElement.getBoundingClientRect(),{width:i,height:a}=this._viewportRuler.getViewportSize();nh(e,[{width:i,height:a,bottom:a,right:i,top:0,left:0}])&&(this.disable(),this._ngZone.run(()=>this._overlayRef.detach()))}})}}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}},Ex=(()=>{class t{_injector=m(X);noop=()=>new mo;close=e=>Cx(this._injector,e);block=()=>kx(this._injector);reposition=e=>Sx(this._injector,e);static \u0275fac=function(i){return new(i||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})}return t})(),Qa=class{positionStrategy;scrollStrategy=new mo;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let e=Object.keys(n);for(let i of e)n[i]!==void 0&&(this[i]=n[i])}}};var xl=class{connectionPair;scrollableViewProperties;constructor(n,e){this.connectionPair=n,this.scrollableViewProperties=e}};var Dx=(()=>{class t{_attachedOverlays=[];_document=m(ee);_isAttached=!1;ngOnDestroy(){this.detach()}add(e){this.remove(e),this._attachedOverlays.push(e)}remove(e){let i=this._attachedOverlays.indexOf(e);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(e,i,a){return a.observers.length<1?!1:e.eventPredicate?e.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})}return t})(),Ix=(()=>{class t extends Dx{_ngZone=m(N);_renderer=m(Je).createRenderer(null,null);_cleanupKeydown;add(e){super.add(e),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=e=>{let i=this._attachedOverlays;for(let a=i.length-1;a>-1;a--){let r=i[a];if(this.canReceiveEvent(r,e,r._keydownEvents)){this._ngZone.run(()=>r._keydownEvents.next(e));break}}};static \u0275fac=function(i){return new(i||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})}return t})(),Nx=(()=>{class t extends Dx{_platform=m(ye);_ngZone=m(N);_renderer=m(Je).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(e){if(super.add(e),!this._isAttached){let i=this._document.body,a={capture:!0},r=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[r.listen(i,"pointerdown",this._pointerDownListener,a),r.listen(i,"click",this._clickListener,a),r.listen(i,"auxclick",this._clickListener,a),r.listen(i,"contextmenu",this._clickListener,a)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(e=>e()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=e=>{this._pointerDownEventTarget=Nt(e)};_clickListener=e=>{let i=Nt(e),a=e.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let r=this._attachedOverlays.slice();for(let o=r.length-1;o>-1;o--){let s=r[o],c=s._outsidePointerEvents;if(!(!s.hasAttached()||!this.canReceiveEvent(s,e,c))){if(vx(s.overlayElement,i)||vx(s.overlayElement,a))break;this._ngZone?this._ngZone.run(()=>c.next(e)):c.next(e)}}};static \u0275fac=function(i){return new(i||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})}return t})();function vx(t,n){let e=typeof ShadowRoot<"u"&&ShadowRoot,i=n;for(;i;){if(i===t)return!0;i=e&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var Mx=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=B({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,a){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
  pointer-events: none;
  top: 0;
  left: 0;
  height: 100%;
  width: 100%;
}

.cdk-overlay-container {
  position: fixed;
}
@layer cdk-overlay {
  .cdk-overlay-container {
    z-index: 1000;
  }
}
.cdk-overlay-container:empty {
  display: none;
}

.cdk-global-overlay-wrapper {
  display: flex;
  position: absolute;
}
@layer cdk-overlay {
  .cdk-global-overlay-wrapper {
    z-index: 1000;
  }
}

.cdk-overlay-pane {
  position: absolute;
  pointer-events: auto;
  box-sizing: border-box;
  display: flex;
  max-width: 100%;
  max-height: 100%;
}
@layer cdk-overlay {
  .cdk-overlay-pane {
    z-index: 1000;
  }
}

.cdk-overlay-backdrop {
  position: absolute;
  top: 0;
  bottom: 0;
  left: 0;
  right: 0;
  pointer-events: auto;
  -webkit-tap-highlight-color: transparent;
  opacity: 0;
  touch-action: manipulation;
}
@layer cdk-overlay {
  .cdk-overlay-backdrop {
    z-index: 1000;
    transition: opacity 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
  }
}
@media (prefers-reduced-motion) {
  .cdk-overlay-backdrop {
    transition-duration: 1ms;
  }
}

.cdk-overlay-backdrop-showing {
  opacity: 1;
}
@media (forced-colors: active) {
  .cdk-overlay-backdrop-showing {
    opacity: 0.6;
  }
}

@layer cdk-overlay {
  .cdk-overlay-dark-backdrop {
    background: rgba(0, 0, 0, 0.32);
  }
}

.cdk-overlay-transparent-backdrop {
  transition: visibility 1ms linear, opacity 1ms linear;
  visibility: hidden;
  opacity: 1;
}
.cdk-overlay-transparent-backdrop.cdk-overlay-backdrop-showing, .cdk-high-contrast-active .cdk-overlay-transparent-backdrop {
  opacity: 0;
  visibility: visible;
}

.cdk-overlay-backdrop-noop-animation {
  transition: none;
}

.cdk-overlay-connected-position-bounding-box {
  position: absolute;
  display: flex;
  flex-direction: column;
  min-width: 1px;
  min-height: 1px;
}
@layer cdk-overlay {
  .cdk-overlay-connected-position-bounding-box {
    z-index: 1000;
  }
}

.cdk-global-scrollblock {
  position: fixed;
  width: 100%;
  overflow-y: scroll;
}

.cdk-overlay-popover {
  background: none;
  border: none;
  padding: 0;
  outline: 0;
  overflow: visible;
  position: fixed;
  pointer-events: none;
  white-space: normal;
  color: inherit;
  text-decoration: none;
  width: 100%;
  height: 100%;
  inset: auto;
  top: 0;
  left: 0;
}
.cdk-overlay-popover::backdrop {
  display: none;
}
.cdk-overlay-popover .cdk-overlay-backdrop {
  position: fixed;
  z-index: auto;
}
`],encapsulation:2})}return t})(),Tx=(()=>{class t{_platform=m(ye);_containerElement;_document=m(ee);_styleLoader=m(je);ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let e="cdk-overlay-container";if(this._platform.isBrowser||vp()){let a=this._document.querySelectorAll(`.${e}[platform="server"], .${e}[platform="test"]`);for(let r=0;r<a.length;r++)a[r].remove()}let i=this._document.createElement("div");i.classList.add(e),vp()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load(Mx)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})}return t})(),ih=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,e,i,a){this._renderer=e,this._ngZone=i,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=e.listen(this.element,"click",a)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function ah(t){return t&&t.nodeType===1}var th=ae([]),wl=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new E;_attachments=new E;_detachments=new E;_positionStrategy;_scrollStrategy;_locationChanges;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new E;_outsidePointerEvents=new E;_afterNextRenderRef;constructor(n,e,i,a,r,o,s,c,l,d=!1,u,g){this._portalOutlet=n,this._host=e,this._pane=i,this._config=a,this._ngZone=r,this._keyboardDispatcher=o,this._document=s,this._location=c,this._outsideClickDispatcher=l,this._animationsDisabled=d,this._injector=u,this._renderer=g,a.scrollStrategy&&(this._scrollStrategy=a.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=a.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let e=this._portalOutlet.attach(n);if(this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),tt(()=>{th.update(i=>i.includes(this)?i:[...i,this])}),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=mt(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation===!0||this._config.disposeOnNavigation==="pop-state"){let i=this._location.subscribe(()=>this.dispose());this._locationChanges=()=>i.unsubscribe()}else this._config.disposeOnNavigation==="url-change"&&(this._locationChanges=this._location.onUrlChange(()=>this.dispose()));return this._outsideClickDispatcher.add(this),typeof e?.onDestroy=="function"&&e.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),e}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges?.(),this._outsideClickDispatcher.remove(this),tt(()=>{th.update(e=>e.filter(i=>i!==this))}),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges?.(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0,tt(()=>{th.update(e=>e.filter(i=>i!==this))})}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=P(P({},this._config),n),this._updateElementSize()}setDirection(n){this._config=ge(P({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=Ze(this._config.width),n.height=Ze(this._config.height),n.minWidth=Ze(this._config.minWidth),n.minHeight=Ze(this._config.minHeight),n.maxWidth=Ze(this._config.maxWidth),n.maxHeight=Ze(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;ah(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch{}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new ih(this._document,this._renderer,this._ngZone,e=>{this._backdropClick.next(e)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,e,i){let a=Ua(e||[]).filter(r=>!!r);a.length&&(i?n.classList.add(...a):n.classList.remove(...a))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=mt(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(e){if(n)throw e;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}},_x="cdk-overlay-connected-position-bounding-box",zN=/([A-Za-z%]+)$/;function Ax(t,n){return new kl(n,t.get(na),t.get(ee),t.get(ye),t.get(Tx))}var kl=class{_viewportRuler;_document;_platform;_overlayContainer;_overlayRef;_isInitialRender=!1;_lastBoundingBoxSize={width:0,height:0};_isPushed=!1;_canPush=!0;_growAfterOpen=!1;_hasFlexibleDimensions=!0;_positionLocked=!1;_originRect;_overlayRect;_viewportRect;_containerRect;_viewportMargin=0;_scrollables=[];_preferredPositions=[];_origin;_pane;_isDisposed=!1;_boundingBox=null;_lastPosition=null;_lastScrollVisibility=null;_positionChanges=new E;_resizeSubscription=pe.EMPTY;_offsetX=0;_offsetY=0;_transformOriginSelector;_appliedPanelClasses=[];_previousPushAmount=null;_popoverLocation="global";positionChanges=this._positionChanges;get positions(){return this._preferredPositions}constructor(n,e,i,a,r){this._viewportRuler=e,this._document=i,this._platform=a,this._overlayContainer=r,this.setOrigin(n)}attach(n){this._overlayRef&&this._overlayRef,this._validatePositions(),n.hostElement.classList.add(_x),this._overlayRef=n,this._boundingBox=n.hostElement,this._pane=n.overlayElement,this._isDisposed=!1,this._isInitialRender=!0,this._lastPosition=null,this._resizeSubscription.unsubscribe(),this._resizeSubscription=this._viewportRuler.change().subscribe(()=>{this._isInitialRender=!0,this.apply()})}apply(){if(this._isDisposed||!this._platform.isBrowser)return;if(!this._isInitialRender&&this._positionLocked&&this._lastPosition){this.reapplyLastPosition();return}this._clearPanelClasses(),this._resetOverlayElementStyles(),this._resetBoundingBoxStyles(),this._viewportRect=this._getNarrowedViewportRect(),this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._containerRect=this._getContainerRect();let n=this._originRect,e=this._overlayRect,i=this._viewportRect,a=this._containerRect,r=[],o;for(let s of this._preferredPositions){let c=this._getOriginPoint(n,a,s),l=this._getOverlayPoint(c,e,s),d=this._getOverlayFit(l,e,i,s);if(d.isCompletelyWithinViewport){this._isPushed=!1,this._applyPosition(s,c);return}if(this._canFitWithFlexibleDimensions(d,l,i)){r.push({position:s,origin:c,overlayRect:e,boundingBoxRect:this._calculateBoundingBoxRect(c,s)});continue}(!o||o.overlayFit.visibleArea<d.visibleArea)&&(o={overlayFit:d,overlayPoint:l,originPoint:c,position:s,overlayRect:e})}if(r.length){let s=null,c=-1;for(let l of r){let d=l.boundingBoxRect.width*l.boundingBoxRect.height*(l.position.weight||1);d>c&&(c=d,s=l)}this._isPushed=!1,this._applyPosition(s.position,s.origin);return}if(this._canPush){this._isPushed=!0,this._applyPosition(o.position,o.originPoint);return}this._applyPosition(o.position,o.originPoint)}detach(){this._clearPanelClasses(),this._lastPosition=null,this._previousPushAmount=null,this._resizeSubscription.unsubscribe()}dispose(){this._isDisposed||(this._boundingBox&&ia(this._boundingBox.style,{top:"",left:"",right:"",bottom:"",height:"",width:"",alignItems:"",justifyContent:""}),this._pane&&this._resetOverlayElementStyles(),this._overlayRef&&this._overlayRef.hostElement.classList.remove(_x),this.detach(),this._positionChanges.complete(),this._overlayRef=this._boundingBox=null,this._isDisposed=!0)}reapplyLastPosition(){if(this._isDisposed||!this._platform.isBrowser)return;let n=this._lastPosition;n?(this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._viewportRect=this._getNarrowedViewportRect(),this._containerRect=this._getContainerRect(),this._applyPosition(n,this._getOriginPoint(this._originRect,this._containerRect,n))):this.apply()}withScrollableContainers(n){return this._scrollables=n,this}withPositions(n){return this._preferredPositions=n,n.indexOf(this._lastPosition)===-1&&(this._lastPosition=null),this._validatePositions(),this}withViewportMargin(n){return this._viewportMargin=n,this}withFlexibleDimensions(n=!0){return this._hasFlexibleDimensions=n,this}withGrowAfterOpen(n=!0){return this._growAfterOpen=n,this}withPush(n=!0){return this._canPush=n,this}withLockedPosition(n=!0){return this._positionLocked=n,this}setOrigin(n){return this._origin=n,this}withDefaultOffsetX(n){return this._offsetX=n,this}withDefaultOffsetY(n){return this._offsetY=n,this}withTransformOriginOn(n){return this._transformOriginSelector=n,this}withPopoverLocation(n){return this._popoverLocation=n,this}getPopoverInsertionPoint(){return this._popoverLocation==="global"?null:this._popoverLocation!=="inline"?this._popoverLocation:this._origin instanceof L?this._origin.nativeElement:ah(this._origin)?this._origin:null}_getOriginPoint(n,e,i){let a;if(i.originX=="center")a=n.left+n.width/2;else{let o=this._isRtl()?n.right:n.left,s=this._isRtl()?n.left:n.right;a=i.originX=="start"?o:s}e.left<0&&(a-=e.left);let r;return i.originY=="center"?r=n.top+n.height/2:r=i.originY=="top"?n.top:n.bottom,e.top<0&&(r-=e.top),{x:a,y:r}}_getOverlayPoint(n,e,i){let a;i.overlayX=="center"?a=-e.width/2:i.overlayX==="start"?a=this._isRtl()?-e.width:0:a=this._isRtl()?0:-e.width;let r;return i.overlayY=="center"?r=-e.height/2:r=i.overlayY=="top"?0:-e.height,{x:n.x+a,y:n.y+r}}_getOverlayFit(n,e,i,a){let r=xx(e),{x:o,y:s}=n,c=this._getOffset(a,"x"),l=this._getOffset(a,"y");c&&(o+=c),l&&(s+=l);let d=0-o,u=o+r.width-i.width,g=0-s,f=s+r.height-i.height,_=this._subtractOverflows(r.width,d,u),w=this._subtractOverflows(r.height,g,f),A=_*w;return{visibleArea:A,isCompletelyWithinViewport:r.width*r.height===A,fitsInViewportVertically:w===r.height,fitsInViewportHorizontally:_==r.width}}_canFitWithFlexibleDimensions(n,e,i){if(this._hasFlexibleDimensions){let a=i.bottom-e.y,r=i.right-e.x,o=yx(this._overlayRef.getConfig().minHeight),s=yx(this._overlayRef.getConfig().minWidth),c=n.fitsInViewportVertically||o!=null&&o<=a,l=n.fitsInViewportHorizontally||s!=null&&s<=r;return c&&l}return!1}_pushOverlayOnScreen(n,e,i){if(this._previousPushAmount&&this._positionLocked)return{x:n.x+this._previousPushAmount.x,y:n.y+this._previousPushAmount.y};let a=xx(e),r=this._viewportRect,o=Math.max(n.x+a.width-r.width,0),s=Math.max(n.y+a.height-r.height,0),c=Math.max(r.top-i.top-n.y,0),l=Math.max(r.left-i.left-n.x,0),d=0,u=0;return a.width<=r.width?d=l||-o:d=n.x<this._getViewportMarginStart()?r.left-i.left-n.x:0,a.height<=r.height?u=c||-s:u=n.y<this._getViewportMarginTop()?r.top-i.top-n.y:0,this._previousPushAmount={x:d,y:u},{x:n.x+d,y:n.y+u}}_applyPosition(n,e){if(this._setTransformOrigin(n),this._setOverlayElementStyles(e,n),this._setBoundingBoxStyles(e,n),n.panelClass&&this._addPanelClasses(n.panelClass),this._positionChanges.observers.length){let i=this._getScrollVisibility();if(n!==this._lastPosition||!this._lastScrollVisibility||!HN(this._lastScrollVisibility,i)){let a=new xl(n,i);this._positionChanges.next(a)}this._lastScrollVisibility=i}this._lastPosition=n,this._isInitialRender=!1}_setTransformOrigin(n){if(!this._transformOriginSelector)return;let e=this._boundingBox.querySelectorAll(this._transformOriginSelector),i,a=n.overlayY;n.overlayX==="center"?i="center":this._isRtl()?i=n.overlayX==="start"?"right":"left":i=n.overlayX==="start"?"left":"right";for(let r=0;r<e.length;r++)e[r].style.transformOrigin=`${i} ${a}`}_calculateBoundingBoxRect(n,e){let i=this._viewportRect,a=this._isRtl(),r,o,s;if(e.overlayY==="top")o=n.y,r=i.height-o+this._getViewportMarginBottom();else if(e.overlayY==="bottom")s=i.height-n.y+this._getViewportMarginTop()+this._getViewportMarginBottom(),r=i.height-s+this._getViewportMarginTop();else{let f=Math.min(i.bottom-n.y+i.top,n.y),_=this._lastBoundingBoxSize.height;r=f*2,o=n.y-f,r>_&&!this._isInitialRender&&!this._growAfterOpen&&(o=n.y-_/2)}let c=e.overlayX==="start"&&!a||e.overlayX==="end"&&a,l=e.overlayX==="end"&&!a||e.overlayX==="start"&&a,d,u,g;if(l)g=i.width-n.x+this._getViewportMarginStart()+this._getViewportMarginEnd(),d=n.x-this._getViewportMarginStart();else if(c)u=n.x,d=i.right-n.x-this._getViewportMarginEnd();else{let f=Math.min(i.right-n.x+i.left,n.x),_=this._lastBoundingBoxSize.width;d=f*2,u=n.x-f,d>_&&!this._isInitialRender&&!this._growAfterOpen&&(u=n.x-_/2)}return{top:o,left:u,bottom:s,right:g,width:d,height:r}}_setBoundingBoxStyles(n,e){let i=this._calculateBoundingBoxRect(n,e);!this._isInitialRender&&!this._growAfterOpen&&(i.height=Math.min(i.height,this._lastBoundingBoxSize.height),i.width=Math.min(i.width,this._lastBoundingBoxSize.width));let a={};if(this._hasExactPosition())a.top=a.left="0",a.bottom=a.right="auto",a.maxHeight=a.maxWidth="",a.width=a.height="100%";else{let r=this._overlayRef.getConfig().maxHeight,o=this._overlayRef.getConfig().maxWidth;a.width=Ze(i.width),a.height=Ze(i.height),a.top=Ze(i.top)||"auto",a.bottom=Ze(i.bottom)||"auto",a.left=Ze(i.left)||"auto",a.right=Ze(i.right)||"auto",e.overlayX==="center"?a.alignItems="center":a.alignItems=e.overlayX==="end"?"flex-end":"flex-start",e.overlayY==="center"?a.justifyContent="center":a.justifyContent=e.overlayY==="bottom"?"flex-end":"flex-start",r&&(a.maxHeight=Ze(r)),o&&(a.maxWidth=Ze(o))}this._lastBoundingBoxSize=i,ia(this._boundingBox.style,a)}_resetBoundingBoxStyles(){ia(this._boundingBox.style,{top:"0",left:"0",right:"0",bottom:"0",height:"",width:"",alignItems:"",justifyContent:""})}_resetOverlayElementStyles(){ia(this._pane.style,{top:"",left:"",bottom:"",right:"",position:"",transform:""})}_setOverlayElementStyles(n,e){let i={},a=this._hasExactPosition(),r=this._hasFlexibleDimensions,o=this._overlayRef.getConfig();if(a){let d=this._viewportRuler.getViewportScrollPosition();ia(i,this._getExactOverlayY(e,n,d)),ia(i,this._getExactOverlayX(e,n,d))}else i.position="static";let s="",c=this._getOffset(e,"x"),l=this._getOffset(e,"y");c&&(s+=`translateX(${c}px) `),l&&(s+=`translateY(${l}px)`),i.transform=s.trim(),o.maxHeight&&(a?i.maxHeight=Ze(o.maxHeight):r&&(i.maxHeight="")),o.maxWidth&&(a?i.maxWidth=Ze(o.maxWidth):r&&(i.maxWidth="")),ia(this._pane.style,i)}_getExactOverlayY(n,e,i){let a={top:"",bottom:""},r=this._getOverlayPoint(e,this._overlayRect,n);if(this._isPushed&&(r=this._pushOverlayOnScreen(r,this._overlayRect,i)),n.overlayY==="bottom"){let o=this._document.documentElement.clientHeight;a.bottom=`${o-(r.y+this._overlayRect.height)}px`}else a.top=Ze(r.y);return a}_getExactOverlayX(n,e,i){let a={left:"",right:""},r=this._getOverlayPoint(e,this._overlayRect,n);this._isPushed&&(r=this._pushOverlayOnScreen(r,this._overlayRect,i));let o;if(this._isRtl()?o=n.overlayX==="end"?"left":"right":o=n.overlayX==="end"?"right":"left",o==="right"){let s=this._document.documentElement.clientWidth;a.right=`${s-(r.x+this._overlayRect.width)}px`}else a.left=Ze(r.x);return a}_getScrollVisibility(){let n=this._getOriginRect(),e=this._pane.getBoundingClientRect(),i=this._scrollables.map(a=>a.getElementRef().nativeElement.getBoundingClientRect());return{isOriginClipped:bx(n,i),isOriginOutsideView:nh(n,i),isOverlayClipped:bx(e,i),isOverlayOutsideView:nh(e,i)}}_subtractOverflows(n,...e){return e.reduce((i,a)=>i-Math.max(a,0),n)}_getNarrowedViewportRect(){let n=this._document.documentElement.clientWidth,e=this._document.documentElement.clientHeight,i=this._viewportRuler.getViewportScrollPosition();return{top:i.top+this._getViewportMarginTop(),left:i.left+this._getViewportMarginStart(),right:i.left+n-this._getViewportMarginEnd(),bottom:i.top+e-this._getViewportMarginBottom(),width:n-this._getViewportMarginStart()-this._getViewportMarginEnd(),height:e-this._getViewportMarginTop()-this._getViewportMarginBottom()}}_isRtl(){return this._overlayRef.getDirection()==="rtl"}_hasExactPosition(){return!this._hasFlexibleDimensions||this._isPushed}_getOffset(n,e){return e==="x"?n.offsetX==null?this._offsetX:n.offsetX:n.offsetY==null?this._offsetY:n.offsetY}_validatePositions(){}_addPanelClasses(n){this._pane&&Ua(n).forEach(e=>{e!==""&&this._appliedPanelClasses.indexOf(e)===-1&&(this._appliedPanelClasses.push(e),this._pane.classList.add(e))})}_clearPanelClasses(){this._pane&&(this._appliedPanelClasses.forEach(n=>{this._pane.classList.remove(n)}),this._appliedPanelClasses=[])}_getViewportMarginStart(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.start??0}_getViewportMarginEnd(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.end??0}_getViewportMarginTop(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.top??0}_getViewportMarginBottom(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.bottom??0}_getOriginRect(){let n=this._origin;if(n instanceof L)return n.nativeElement.getBoundingClientRect();if(n instanceof Element)return n.getBoundingClientRect();let e=n.width||0,i=n.height||0;return{top:n.y,bottom:n.y+i,left:n.x,right:n.x+e,height:i,width:e}}_getContainerRect(){let n=this._overlayRef.getConfig().usePopover&&this._popoverLocation!=="global",e=this._overlayContainer.getContainerElement();n&&(e.style.display="block");let i=e.getBoundingClientRect();return n&&(e.style.display=""),i}};function ia(t,n){for(let e in n)Object.hasOwn(n,e)&&(t[e]=n[e]);return t}function yx(t){if(typeof t!="number"&&t!=null){let[n,e]=t.split(zN);return!e||e==="px"?parseFloat(n):null}return t||null}function xx(t){return{top:Math.floor(t.top),right:Math.floor(t.right),bottom:Math.floor(t.bottom),left:Math.floor(t.left),width:Math.floor(t.width),height:Math.floor(t.height)}}function HN(t,n){return t===n?!0:t.isOriginClipped===n.isOriginClipped&&t.isOriginOutsideView===n.isOriginOutsideView&&t.isOverlayClipped===n.isOverlayClipped&&t.isOverlayOutsideView===n.isOverlayOutsideView}var wx="cdk-global-overlay-wrapper";function Sl(t){return new Cl}var Cl=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let e=n.getConfig();this._overlayRef=n,this._width&&!e.width&&n.updateSize({width:this._width}),this._height&&!e.height&&n.updateSize({height:this._height}),n.hostElement.classList.add(wx),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:a,height:r,maxWidth:o,maxHeight:s}=i,c=(a==="100%"||a==="100vw")&&(!o||o==="100%"||o==="100vw"),l=(r==="100%"||r==="100vh")&&(!s||s==="100%"||s==="100vh"),d=this._xPosition,u=this._xOffset,g=this._overlayRef.getConfig().direction==="rtl",f="",_="",w="";c?w="flex-start":d==="center"?(w="center",g?_=u:f=u):g?d==="left"||d==="end"?(w="flex-end",f=u):(d==="right"||d==="start")&&(w="flex-start",_=u):d==="left"||d==="start"?(w="flex-start",f=u):(d==="right"||d==="end")&&(w="flex-end",_=u),n.position=this._cssPosition,n.marginLeft=c?"0":f,n.marginTop=l?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=c?"0":_,e.justifyContent=w,e.alignItems=l?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement,i=e.style;e.classList.remove(wx),i.justifyContent=i.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}},Rx=(()=>{class t{_injector=m(X);global(){return Sl()}flexibleConnectedTo(e){return Ax(this._injector,e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})}return t})(),Ox=new y("OVERLAY_DEFAULT_CONFIG");function El(t,n){t.get(je).load(Mx);let e=t.get(Tx),i=t.get(ee),a=t.get(Pe),r=t.get(rn),o=t.get(Mt),s=t.get(Se,null,{optional:!0})||t.get(Je).createRenderer(null,null),c=new Qa(n),l=t.get(Ox,null,{optional:!0})?.usePopover??!0;c.direction=c.direction||o.value,!i.body||!("showPopover"in i.body)?c.usePopover=!1:c.usePopover=n?.usePopover??l;let d=i.createElement("div"),u=i.createElement("div");d.id=a.getId("cdk-overlay-"),d.classList.add("cdk-overlay-pane"),u.appendChild(d),c.usePopover&&(u.setAttribute("popover","manual"),u.classList.add("cdk-overlay-popover"));let g=c.usePopover?c.positionStrategy?.getPopoverInsertionPoint?.():null;return ah(g)?g.after(u):g?.type==="parent"?g.element.appendChild(u):e.getContainerElement().appendChild(u),new wl(new fl(d,r,t),u,d,c,t.get(N),t.get(Ix),i,t.get(yc),t.get(Nx),n?.disableAnimations??t.get(xr,null,{optional:!0})==="NoopAnimations",t.get(ct),s)}var Fx=(()=>{class t{scrollStrategies=m(Ex);_positionBuilder=m(Rx);_injector=m(X);create(e){return El(this._injector,e)}position(){return this._positionBuilder}static \u0275fac=function(i){return new(i||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})}return t})();var rh=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=G({providers:[Fx],imports:[me,gl,Yp,Yp]})}return t})();var UN=Math.pow(2,31)-1,uo=class{_overlayRef;instance;containerInstance;_afterDismissed=new E;_afterOpened=new E;_onAction=new E;_durationTimeoutId;_dismissedByAction=!1;constructor(n,e){this._overlayRef=e,this.containerInstance=n,n._onExit.subscribe(()=>this._finishDismiss())}dismiss(){this._afterDismissed.closed||this.containerInstance.exit(),clearTimeout(this._durationTimeoutId)}dismissWithAction(){this._onAction.closed||(this._dismissedByAction=!0,this._onAction.next(),this._onAction.complete(),this.dismiss()),clearTimeout(this._durationTimeoutId)}closeWithAction(){this.dismissWithAction()}_dismissAfter(n){this._durationTimeoutId=setTimeout(()=>this.dismiss(),Math.min(n,UN))}_open(){this._afterOpened.closed||(this._afterOpened.next(),this._afterOpened.complete())}_finishDismiss(){this._overlayRef.dispose(),this._onAction.closed||this._onAction.complete(),this._afterDismissed.next({dismissedByAction:this._dismissedByAction}),this._afterDismissed.complete(),this._dismissedByAction=!1}afterDismissed(){return this._afterDismissed}afterOpened(){return this.containerInstance._onEnter}onAction(){return this._onAction}},Lx=new y("MatSnackBarData"),Xa=class{politeness="polite";announcementMessage="";viewContainerRef;duration=0;panelClass;direction;data=null;horizontalPosition="center";verticalPosition="bottom"},$N=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["","matSnackBarLabel",""]],hostAttrs:[1,"mat-mdc-snack-bar-label","mdc-snackbar__label"]})}return t})(),GN=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["","matSnackBarActions",""]],hostAttrs:[1,"mat-mdc-snack-bar-actions","mdc-snackbar__actions"]})}return t})(),WN=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=D({type:t,selectors:[["","matSnackBarAction",""]],hostAttrs:[1,"mat-mdc-snack-bar-action","mdc-snackbar__action"]})}return t})(),Px=(()=>{class t{snackBarRef=m(uo);data=m(Lx);action(){this.snackBarRef.dismissWithAction()}get hasAction(){return!!this.data.action}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){function e(i,a){if(i&1){let r=ve();p(0,"div",1)(1,"button",2),C("click",function(){R(r);let s=x();return O(s.action())}),b(2),h()()}if(i&2){let r=x();v(2),zn(" ",r.data.action," ")}}return B({type:t,selectors:[["simple-snack-bar"]],hostAttrs:[1,"mat-mdc-simple-snack-bar"],exportAs:["matSnackBar"],decls:3,vars:2,consts:[["matSnackBarLabel",""],["matSnackBarActions",""],["matButton","","matSnackBarAction","",3,"click"]],template:function(a,r){a&1&&(p(0,"div",0),b(1),h(),te(2,e,3,1,"div",1)),a&2&&(v(),zn(" ",r.data.message,`
`),v(),ne(r.hasAction?2:-1))},dependencies:[el,$N,GN,WN],styles:[`.mat-mdc-simple-snack-bar {
  display: flex;
}
.mat-mdc-simple-snack-bar .mat-mdc-snack-bar-label {
  max-height: 50vh;
  overflow: auto;
}
`],encapsulation:2})})()}return t})(),oh="_mat-snack-bar-enter",sh="_mat-snack-bar-exit",qN=(()=>{class t extends Za{_ngZone=m(N);_elementRef=m(L);_changeDetectorRef=m(Ie);_platform=m(ye);_animationsDisabled=Re();snackBarConfig=m(Xa);_document=m(ee);_trackedModals=new Set;_enterFallback;_exitFallback;_injector=m(X);_announceDelay=150;_announceTimeoutId;_destroyed=!1;_portalOutlet;_onAnnounce=new E;_onExit=new E;_onEnter=new E;_animationState="void";_live;_label;_role;_liveElementId=m(Pe).getId("mat-snack-bar-container-live-");constructor(){super();let e=this.snackBarConfig;e.politeness==="assertive"&&!e.announcementMessage?this._live="assertive":e.politeness==="off"?this._live="off":this._live="polite",this._platform.FIREFOX&&(this._live==="polite"&&(this._role="status"),this._live==="assertive"&&(this._role="alert"))}attachComponentPortal(e){this._assertNotAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._afterPortalAttached(),i}attachTemplatePortal(e){this._assertNotAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._afterPortalAttached(),i}attachDomPortal=e=>{this._assertNotAttached();let i=this._portalOutlet.attachDomPortal(e);return this._afterPortalAttached(),i};onAnimationEnd(e){e===sh?this._completeExit():e===oh&&(clearTimeout(this._enterFallback),this._ngZone.run(()=>{this._onEnter.next(),this._onEnter.complete()}))}enter(){this._destroyed||(this._animationState="visible",this._changeDetectorRef.markForCheck(),this._changeDetectorRef.detectChanges(),this._screenReaderAnnounce(),this._animationsDisabled?mt(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(oh)))},{injector:this._injector}):(clearTimeout(this._enterFallback),this._enterFallback=setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-snack-bar-fallback-visible"),this.onAnimationEnd(oh)},200)))}exit(){return this._destroyed?In(void 0):(this._ngZone.run(()=>{this._animationState="hidden",this._changeDetectorRef.markForCheck(),this._elementRef.nativeElement.setAttribute("mat-exit",""),clearTimeout(this._announceTimeoutId),this._animationsDisabled?mt(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(sh)))},{injector:this._injector}):(clearTimeout(this._exitFallback),this._exitFallback=setTimeout(()=>this.onAnimationEnd(sh),200))}),this._onExit)}ngOnDestroy(){this._destroyed=!0,this._clearFromModals(),this._completeExit()}_completeExit(){clearTimeout(this._exitFallback),queueMicrotask(()=>{this._onExit.next(),this._onExit.complete()})}_afterPortalAttached(){let e=this._elementRef.nativeElement,i=this.snackBarConfig.panelClass;i&&(Array.isArray(i)?i.forEach(o=>e.classList.add(o)):e.classList.add(i)),this._exposeToModals();let a=this._label.nativeElement,r="mdc-snackbar__label";a.classList.toggle(r,!a.querySelector(`.${r}`))}_exposeToModals(){let e=this._liveElementId,i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let a=0;a<i.length;a++){let r=i[a],o=r.getAttribute("aria-owns");this._trackedModals.add(r),o?o.indexOf(e)===-1&&r.setAttribute("aria-owns",o+" "+e):r.setAttribute("aria-owns",e)}}_clearFromModals(){this._trackedModals.forEach(e=>{let i=e.getAttribute("aria-owns");if(i){let a=i.replace(this._liveElementId,"").trim();a.length>0?e.setAttribute("aria-owns",a):e.removeAttribute("aria-owns")}}),this._trackedModals.clear()}_assertNotAttached(){this._portalOutlet.hasAttached()}_screenReaderAnnounce(){this._announceTimeoutId||this._ngZone.runOutsideAngular(()=>{this._announceTimeoutId=setTimeout(()=>{if(this._destroyed)return;let e=this._elementRef.nativeElement,i=e.querySelector("[aria-hidden]"),a=e.querySelector("[aria-live]");if(i&&a){let r=null;this._platform.isBrowser&&document.activeElement instanceof HTMLElement&&i.contains(document.activeElement)&&(r=document.activeElement),i.removeAttribute("aria-hidden"),a.appendChild(i),r?.focus(),this._onAnnounce.next(),this._onAnnounce.complete()}},this._announceDelay)})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=(function(){let e=["label"];function i(a,r){}return B({type:t,selectors:[["mat-snack-bar-container"]],viewQuery:function(r,o){if(r&1&&Be(Ka,7)(e,7),r&2){let s;M(s=T())&&(o._portalOutlet=s.first),M(s=T())&&(o._label=s.first)}},hostAttrs:[1,"mdc-snackbar","mat-mdc-snack-bar-container"],hostVars:6,hostBindings:function(r,o){r&1&&C("animationend",function(c){return o.onAnimationEnd(c.animationName)})("animationcancel",function(c){return o.onAnimationEnd(c.animationName)}),r&2&&S("mat-snack-bar-container-enter",o._animationState==="visible")("mat-snack-bar-container-exit",o._animationState==="hidden")("mat-snack-bar-container-animations-enabled",!o._animationsDisabled)},features:[le],decls:6,vars:3,consts:[["label",""],[1,"mdc-snackbar__surface","mat-mdc-snackbar-surface"],[1,"mat-mdc-snack-bar-label"],["aria-hidden","true"],["cdkPortalOutlet",""]],template:function(r,o){r&1&&(p(0,"div",1)(1,"div",2,0)(3,"div",3),It(4,i,0,0,"ng-template",4),h(),$(5,"div"),h()()),r&2&&(v(5),K("aria-live",o._live)("role",o._role)("id",o._liveElementId))},dependencies:[Ka],styles:[`@keyframes _mat-snack-bar-enter {
  from {
    transform: scale(0.8);
    opacity: 0;
  }
  to {
    transform: scale(1);
    opacity: 1;
  }
}
@keyframes _mat-snack-bar-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-snack-bar-container {
  display: flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  -webkit-tap-highlight-color: rgba(0, 0, 0, 0);
  margin: 8px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snack-bar-container {
  width: 100vw;
}

.mat-snack-bar-container-animations-enabled {
  opacity: 0;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-fallback-visible {
  opacity: 1;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-enter {
  animation: _mat-snack-bar-enter 150ms cubic-bezier(0, 0, 0.2, 1) forwards;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-exit {
  animation: _mat-snack-bar-exit 75ms cubic-bezier(0.4, 0, 1, 1) forwards;
}

.mat-mdc-snackbar-surface {
  box-shadow: 0px 3px 5px -1px rgba(0, 0, 0, 0.2), 0px 6px 10px 0px rgba(0, 0, 0, 0.14), 0px 1px 18px 0px rgba(0, 0, 0, 0.12);
  display: flex;
  align-items: center;
  justify-content: flex-start;
  box-sizing: border-box;
  padding-left: 0;
  padding-right: 8px;
}
[dir=rtl] .mat-mdc-snackbar-surface {
  padding-right: 0;
  padding-left: 8px;
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  min-width: 344px;
  max-width: 672px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snackbar-surface {
  width: 100%;
  min-width: 0;
}
@media (forced-colors: active) {
  .mat-mdc-snackbar-surface {
    outline: solid 1px;
  }
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  color: var(--%NS%mat-snack-bar-supporting-text-color, var(--%NS%mat-sys-inverse-on-surface));
  border-radius: var(--%NS%mat-snack-bar-container-shape, var(--%NS%mat-sys-corner-extra-small));
  background-color: var(--%NS%mat-snack-bar-container-color, var(--%NS%mat-sys-inverse-surface));
}

.mdc-snackbar__label {
  width: 100%;
  flex-grow: 1;
  box-sizing: border-box;
  margin: 0;
  padding: 14px 8px 14px 16px;
}
[dir=rtl] .mdc-snackbar__label {
  padding-left: 8px;
  padding-right: 16px;
}
.mat-mdc-snack-bar-container .mdc-snackbar__label {
  font-family: var(--%NS%mat-snack-bar-supporting-text-font, var(--%NS%mat-sys-body-medium-font));
  font-size: var(--%NS%mat-snack-bar-supporting-text-size, var(--%NS%mat-sys-body-medium-size));
  font-weight: var(--%NS%mat-snack-bar-supporting-text-weight, var(--%NS%mat-sys-body-medium-weight));
  line-height: var(--%NS%mat-snack-bar-supporting-text-line-height, var(--%NS%mat-sys-body-medium-line-height));
}

.mat-mdc-snack-bar-actions {
  display: flex;
  flex-shrink: 0;
  align-items: center;
  box-sizing: border-box;
}

.mat-mdc-snack-bar-handset,
.mat-mdc-snack-bar-container,
.mat-mdc-snack-bar-label {
  flex: 1 1 auto;
}

.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled).mat-unthemed {
  color: var(--%NS%mat-snack-bar-button-color, var(--%NS%mat-sys-inverse-primary));
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) {
  --%NS%mat-button-text-state-layer-color: currentColor;
  --%NS%mat-button-text-ripple-color: currentColor;
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) .mat-ripple-element {
  opacity: 0.1;
}
`],encapsulation:2,changeDetection:1})})()}return t})(),YN=new y("mat-snack-bar-default-options",{providedIn:"root",factory:()=>new Xa}),ch=(()=>{class t{_live=m(bp);_injector=m(X);_breakpointObserver=m(gp);_parentSnackBar=m(t,{optional:!0,skipSelf:!0});_defaultConfig=m(YN);_animationsDisabled=Re();_snackBarRefAtThisLevel=null;simpleSnackBarComponent=Px;snackBarContainerComponent=qN;handsetCssClass="mat-mdc-snack-bar-handset";get _openedSnackBarRef(){let e=this._parentSnackBar;return e?e._openedSnackBarRef:this._snackBarRefAtThisLevel}set _openedSnackBarRef(e){this._parentSnackBar?this._parentSnackBar._openedSnackBarRef=e:this._snackBarRefAtThisLevel=e}openFromComponent(e,i){return this._attach(e,i)}openFromTemplate(e,i){return this._attach(e,i)}open(e,i="",a){let r=P(P({},this._defaultConfig),a);return r.data={message:e,action:i},r.announcementMessage===e&&(r.announcementMessage=void 0),this.openFromComponent(this.simpleSnackBarComponent,r)}dismiss(){this._openedSnackBarRef&&this._openedSnackBarRef.dismiss()}ngOnDestroy(){this._snackBarRefAtThisLevel&&this._snackBarRefAtThisLevel.dismiss()}_attachSnackBarContainer(e,i){let a=i&&i.viewContainerRef&&i.viewContainerRef.injector,r=X.create({parent:a||this._injector,providers:[{provide:Xa,useValue:i}]}),o=new Ya(this.snackBarContainerComponent,i.viewContainerRef,r),s=e.attach(o);return s.instance.snackBarConfig=i,s.instance}_attach(e,i){let a=P(P(P({},new Xa),this._defaultConfig),i),r=this._createOverlay(a),o=this._attachSnackBarContainer(r,a),s=new uo(o,r);if(e instanceof bt){let c=new Wn(e,null,{$implicit:a.data,snackBarRef:s});s.instance=o.attachTemplatePortal(c)}else{let c=this._createInjector(a,s),l=new Ya(e,void 0,c),d=o.attachComponentPortal(l);s.instance=d.instance}return this._breakpointObserver.observe(ay.HandsetPortrait).pipe(Oe(r.detachments())).subscribe(c=>{r.overlayElement.classList.toggle(this.handsetCssClass,c.matches)}),a.announcementMessage&&o._onAnnounce.subscribe(()=>{this._live.announce(a.announcementMessage,a.politeness)}),this._animateSnackBar(s,a),this._openedSnackBarRef=s,this._openedSnackBarRef}_animateSnackBar(e,i){e.afterDismissed().subscribe(()=>{this._openedSnackBarRef==e&&(this._openedSnackBarRef=null),i.announcementMessage&&this._live.clear()}),i.duration&&i.duration>0&&e.afterOpened().subscribe(()=>e._dismissAfter(i.duration)),this._openedSnackBarRef?(this._openedSnackBarRef.afterDismissed().subscribe(()=>{e.containerInstance.enter()}),this._openedSnackBarRef.dismiss()):e.containerInstance.enter()}_createOverlay(e){let i=new Qa;i.direction=e.direction;let a=Sl(this._injector),r=e.direction==="rtl",o=e.horizontalPosition==="left"||e.horizontalPosition==="start"&&!r||e.horizontalPosition==="end"&&r,s=!o&&e.horizontalPosition!=="center";return o?a.left("0"):s?a.right("0"):a.centerHorizontally(),e.verticalPosition==="top"?a.top("0"):a.bottom("0"),i.positionStrategy=a,i.disableAnimations=this._animationsDisabled,El(this._injector,i)}_createInjector(e,i){let a=e&&e.viewContainerRef&&e.viewContainerRef.injector;return X.create({parent:a||this._injector,providers:[{provide:uo,useValue:i},{provide:Lx,useValue:e.data}]})}static \u0275fac=function(i){return new(i||t)};static \u0275prov=Y({token:t,factory:t.\u0275fac})}return t})();var Vx=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=G({providers:[ch],imports:[rh,gl,tl,Px,me]})}return t})();var Bx=[{nombre:"Enviada",ayuda:"Ha llegado a Inform\xE1tica"},{nombre:"En revisi\xF3n",ayuda:"Inform\xE1tica la clasifica y prioriza"},{nombre:"Asignada",ayuda:"Ya tiene una persona responsable"},{nombre:"En curso",ayuda:"El t\xE9cnico est\xE1 trabajando en ella"},{nombre:"Resuelta",ayuda:"Solucionada"}],jx=[{id:"tpv1",nombre:"TPV \xB7 Caja 1",zona:"L\xEDnea de cajas"},{id:"tpv2",nombre:"TPV \xB7 Caja 2",zona:"L\xEDnea de cajas"},{id:"bascula",nombre:"B\xE1scula fruter\xEDa",zona:"Secci\xF3n frescos"},{id:"impresora",nombre:"Impresora de etiquetas",zona:"Almac\xE9n"},{id:"pda",nombre:"PDA de reposici\xF3n 03",zona:"Almac\xE9n"}],lh={tpv:["No lee tarjetas","No enciende","Pantalla congelada","No imprime el ticket","Mensaje de error","Va muy lento","Otro"],otro:["No enciende","Mensaje de error","Va muy lento","No imprime","Otro"]},zx=[{ref:"INC-20455",dispositivo:"B\xE1scula fruter\xEDa",sintoma:"No pesa bien",cuando:"hace 2 h",estado:"Asignada",nuevo:!0},{ref:"INC-20430",dispositivo:"TPV \xB7 Caja 1",sintoma:"Pantalla congelada",cuando:"ayer",estado:"En curso"},{ref:"INC-20412",dispositivo:"Impresora de etiquetas",sintoma:"No imprime",cuando:"hace 2 d\xEDas",estado:"En revisi\xF3n"}],Hx=[{ref:"INC-20377",dispositivo:"PDA de reposici\xF3n 03",sintoma:"No enciende",cuando:"hace 5 d\xEDas",estado:"Resuelta"}];var ZN=(t,n)=>[t,n],Ux=t=>({actual:t}),$x=(t,n)=>n.nombre,Gx=(t,n)=>n.id,dh=(t,n)=>n.ref;function KN(t,n){if(t&1){let e=ve();p(0,"button",11),C("click",function(){R(e);let a=x(2);return O(a.loTengo())}),b(1,"Ya lo s\xE9"),h()}}function QN(t,n){t&1&&b(0," La Caja 2 no lee las tarjetas de los clientes y no puedes cobrar. Hay clientes esperando. Rep\xF3rtalo con la app. ")}function XN(t,n){t&1&&b(0," Esta ma\xF1ana se avis\xF3 de que la b\xE1scula de fruter\xEDa no pesa bien. Averigua en qu\xE9 estado est\xE1 esa incidencia y pulsa \xABYa lo s\xE9\xBB. ")}function JN(t,n){if(t&1&&(p(0,"p",10),te(1,QN,1,0)(2,XN,1,0),h()),t&2){let e=x(2);v(),ne(e.tarea()===1?1:2)}}function eM(t,n){if(t&1){let e=ve();p(0,"div",4)(1,"span",7),b(2),h(),p(3,"button",8),C("click",function(){R(e);let a=x();return O(a.verTarea.set(!a.verTarea()))}),b(4),h(),p(5,"button",8),C("click",function(){R(e);let a=x();return O(a.rendirse())}),b(6,"No s\xE9 seguir"),h(),te(7,KN,2,0,"button",9),te(8,JN,3,1,"p",10),h()}if(t&2){let e=x();v(2),si("Tarea ",e.tarea(),"/2 \xB7 ",e.tarea()===1?"Caja 2 no lee tarjetas":"Estado de la b\xE1scula"),v(2),de(e.verTarea()?"Ocultar":"Ver tarea"),v(3),ne(e.tarea()===2?7:-1),v(),ne(e.verTarea()?8:-1)}}function tM(t,n){if(t&1){let e=ve();p(0,"main",5)(1,"p",12),b(2,"Prueba r\xE1pida \xB7 5 minutos"),h(),p(3,"h1",13),b(4,"App de soporte t\xE9cnico en tienda"),h(),p(5,"p"),b(6,"Gracias por ayudar. Vas a usar un prototipo para hacer "),p(7,"b"),b(8,"2 tareas cortas"),h(),b(9,". No hay respuestas buenas ni malas: evaluamos la app, no a ti."),h(),p(10,"mat-card",14)(11,"mat-card-content")(12,"ul",15)(13,"li"),b(14,"Imagina que trabajas en una tienda y algo deja de funcionar."),h(),p(15,"li"),b(16,"Medimos el tiempo y por d\xF3nde pasas. No se graba la pantalla ni la voz."),h(),p(17,"li"),b(18,"Nada se env\xEDa solo: al final t\xFA decides si mandas los resultados por WhatsApp."),h(),p(19,"li"),b(20,"No escribas datos personales en las respuestas."),h()()()(),p(21,"mat-form-field",16)(22,"mat-label"),b(23,"Iniciales o apodo (opcional)"),h(),p(24,"input",17),vn(),kn("ngModelChange",function(a){R(e);let r=x();return Hn(r.alias,a)||(r.alias=a),O(a)}),h()(),p(25,"mat-checkbox",18),vn(),kn("ngModelChange",function(a){R(e);let r=x();return Hn(r.consentimiento,a)||(r.consentimiento=a),O(a)}),b(26,"Acepto participar y que se usen mis respuestas, sin datos personales, para mejorar el dise\xF1o."),h(),p(27,"p",19),b(28,"Prototipo de un caso de estudio de dise\xF1o. No es una app oficial de ninguna empresa. Los datos que ver\xE1s son ficticios."),h(),p(29,"div",20)(30,"button",21),C("click",function(){R(e);let a=x();return O(a.empezar())}),b(31,"Empezar"),h()()()}if(t&2){let e=x();v(24),wn("ngModel",e.alias),_n(),v(),wn("ngModel",e.consentimiento),_n(),v(5),F("disabled",!e.consentimiento)}}function nM(t,n){if(t&1){let e=ve();p(0,"main",5)(1,"p",12),b(2,"Tarea 1 de 2"),h(),p(3,"h1",13),b(4,"Reporta una aver\xEDa"),h(),p(5,"mat-card",22)(6,"mat-card-content")(7,"p",23),b(8,"La "),p(9,"b"),b(10,"Caja 2"),h(),b(11," no lee las tarjetas de los clientes y no puedes cobrar. Hay clientes esperando."),h(),p(12,"p",23),b(13,"Rep\xF3rtalo con la app."),h()()(),p(14,"p",19),b(15,"Puedes volver a leer la tarea en la barra superior (\xABVer tarea\xBB). Si te atascas, pulsa \xABNo s\xE9 seguir\xBB."),h(),p(16,"div",20)(17,"button",24),C("click",function(){R(e);let a=x();return O(a.empezarTarea(1))}),b(18,"Empezar tarea 1"),h()()()}}function iM(t,n){if(t&1&&(p(0,"mat-button-toggle",26),b(1),h()),t&2){let e=n.$implicit;F("value",e),v(),de(e)}}function aM(t,n){if(t&1){let e=ve();p(0,"main",5)(1,"p",12),b(2,"Tarea 1 \xB7 terminada"),h(),p(3,"h1",13),b(4,"\xBFQu\xE9 tan f\xE1cil o dif\xEDcil te ha resultado?"),h(),p(5,"mat-button-toggle-group",25),C("change",function(a){R(e);let r=x();return O(r.r.t1.seq=a.value)}),ut(6,iM,2,2,"mat-button-toggle",26,yn),h(),p(8,"div",27)(9,"span"),b(10,"1 \xB7 Muy dif\xEDcil"),h(),p(11,"span"),b(12,"7 \xB7 Muy f\xE1cil"),h()(),p(13,"div",20)(14,"button",21),C("click",function(){R(e);let a=x();return O(a.ir("tarea2"))}),b(15,"Siguiente"),h()()()}if(t&2){let e=x();v(5),F("value",e.r.t1.seq),v(),pt(e.escala7),v(8),F("disabled",e.r.t1.seq===null)}}function rM(t,n){if(t&1){let e=ve();p(0,"main",5)(1,"p",12),b(2,"Tarea 2 de 2"),h(),p(3,"h1",13),b(4,"Consulta una incidencia"),h(),p(5,"mat-card",22)(6,"mat-card-content")(7,"p",23),b(8,"Esta ma\xF1ana se avis\xF3 de que la "),p(9,"b"),b(10,"b\xE1scula de fruter\xEDa"),h(),b(11," no pesa bien."),h(),p(12,"p",23),b(13,"Averigua en qu\xE9 estado est\xE1 esa incidencia. Cuando lo sepas, pulsa \xABYa lo s\xE9\xBB."),h()()(),p(14,"div",20)(15,"button",24),C("click",function(){R(e);let a=x();return O(a.empezarTarea(2))}),b(16,"Empezar tarea 2"),h()()()}}function oM(t,n){if(t&1&&(p(0,"mat-radio-button",26),b(1),h()),t&2){let e=n.$implicit;F("value",e.nombre),v(),de(e.nombre)}}function sM(t,n){if(t&1){let e=ve();p(0,"main",5)(1,"p",12),b(2,"Tarea 2 \xB7 preguntas"),h(),p(3,"h2",28),b(4,"\xBFEn qu\xE9 estado est\xE1 la incidencia de la b\xE1scula?"),h(),p(5,"mat-radio-group",29),vn(),kn("ngModelChange",function(a){R(e);let r=x();return Hn(r.r.t2.estado,a)||(r.r.t2.estado=a),O(a)}),ut(6,oM,2,2,"mat-radio-button",26,$x),p(8,"mat-radio-button",30),b(9,"No lo encontr\xE9"),h()(),p(10,"h2",28),b(11,"Si quisieras, \xBFpodr\xEDas cancelarla ahora?"),h(),p(12,"mat-button-toggle-group",31),C("change",function(a){R(e);let r=x();return O(r.r.t2.cancelar=a.value)}),p(13,"mat-button-toggle",32),b(14,"S\xED"),h(),p(15,"mat-button-toggle",33),b(16,"No"),h(),p(17,"mat-button-toggle",34),b(18,"No lo s\xE9"),h()(),p(19,"mat-form-field",35)(20,"mat-label"),b(21,"Con tus palabras, \xBFqu\xE9 significa ese estado?"),h(),p(22,"textarea",36),vn(),kn("ngModelChange",function(a){R(e);let r=x();return Hn(r.r.t2.significado,a)||(r.r.t2.significado=a),O(a)}),h()(),p(23,"div",20)(24,"button",21),C("click",function(){R(e);let a=x();return O(a.responderT2())}),b(25,"Siguiente"),h()()()}if(t&2){let e=x();v(5),wn("ngModel",e.r.t2.estado),_n(),v(),pt(e.estados),v(6),F("value",e.r.t2.cancelar),v(10),wn("ngModel",e.r.t2.significado),_n(),v(2),F("disabled",!e.r.t2.estado||!e.r.t2.cancelar)}}function cM(t,n){if(t&1&&(p(0,"mat-button-toggle",26),b(1),h()),t&2){let e=n.$implicit;F("value",e),v(),de(e)}}function lM(t,n){if(t&1){let e=ve();p(0,"main",5)(1,"p",12),b(2,"Tarea 2 \xB7 terminada"),h(),p(3,"h1",13),b(4,"\xBFQu\xE9 tan f\xE1cil o dif\xEDcil te ha resultado?"),h(),p(5,"mat-button-toggle-group",25),C("change",function(a){R(e);let r=x();return O(r.r.t2.seq=a.value)}),ut(6,cM,2,2,"mat-button-toggle",26,yn),h(),p(8,"div",27)(9,"span"),b(10,"1 \xB7 Muy dif\xEDcil"),h(),p(11,"span"),b(12,"7 \xB7 Muy f\xE1cil"),h()(),p(13,"div",20)(14,"button",21),C("click",function(){R(e);let a=x();return O(a.ir("umux"))}),b(15,"Siguiente"),h()()()}if(t&2){let e=x();v(5),F("value",e.r.t2.seq),v(),pt(e.escala7),v(8),F("disabled",e.r.t2.seq===null)}}function dM(t,n){if(t&1&&(p(0,"mat-button-toggle",26),b(1),h()),t&2){let e=n.$implicit;F("value",e),v(),de(e)}}function mM(t,n){if(t&1&&(p(0,"mat-button-toggle",26),b(1),h()),t&2){let e=n.$implicit;F("value",e),v(),de(e)}}function uM(t,n){if(t&1){let e=ve();p(0,"main",5)(1,"p",12),b(2,"Para terminar"),h(),p(3,"h2",28),b(4,"\xABEsta app tiene lo que necesitar\xEDa para avisar de una aver\xEDa.\xBB"),h(),p(5,"mat-button-toggle-group",37),C("change",function(a){R(e);let r=x();return O(r.r.umux.capacidades=a.value)}),ut(6,dM,2,2,"mat-button-toggle",26,yn),h(),p(8,"div",27)(9,"span"),b(10,"1 \xB7 Nada de acuerdo"),h(),p(11,"span"),b(12,"7 \xB7 Totalmente de acuerdo"),h()(),p(13,"h2",28),b(14,"\xABEsta app es f\xE1cil de usar.\xBB"),h(),p(15,"mat-button-toggle-group",37),C("change",function(a){R(e);let r=x();return O(r.r.umux.facilidad=a.value)}),ut(16,mM,2,2,"mat-button-toggle",26,yn),h(),p(18,"div",27)(19,"span"),b(20,"1 \xB7 Nada de acuerdo"),h(),p(21,"span"),b(22,"7 \xB7 Totalmente de acuerdo"),h()(),p(23,"mat-form-field",35)(24,"mat-label"),b(25,"\xBFQu\xE9 te ha resultado m\xE1s confuso? (opcional)"),h(),p(26,"textarea",38),vn(),kn("ngModelChange",function(a){R(e);let r=x();return Hn(r.r.confuso,a)||(r.r.confuso=a),O(a)}),h()(),p(27,"mat-form-field",16)(28,"mat-label"),b(29,"\xBFQu\xE9 cambiar\xEDas? (opcional)"),h(),p(30,"textarea",38),vn(),kn("ngModelChange",function(a){R(e);let r=x();return Hn(r.r.mejorar,a)||(r.r.mejorar=a),O(a)}),h()(),p(31,"div",20)(32,"button",21),C("click",function(){R(e);let a=x();return O(a.ir("fin"))}),b(33,"Ver mis resultados"),h()()()}if(t&2){let e=x();v(5),F("value",e.r.umux.capacidades),v(),pt(e.escala7),v(9),F("value",e.r.umux.facilidad),v(),pt(e.escala7),v(10),wn("ngModel",e.r.confuso),_n(),v(4),wn("ngModel",e.r.mejorar),_n(),v(2),F("disabled",e.r.umux.capacidades===null||e.r.umux.facilidad===null)}}function pM(t,n){if(t&1){let e=ve();p(0,"main",5)(1,"p",12),b(2,"\xA1Gracias!"),h(),p(3,"h1",13),b(4,"Env\xEDa tus resultados"),h(),p(5,"p"),b(6,"Pulsa \xABEnviar por WhatsApp\xBB y elige a quien te pas\xF3 el enlace. El mensaje ya va escrito; no lo edites. Si WhatsApp no se abre, pulsa \xABCopiar resultados\xBB y p\xE9galo en el chat."),h(),p(7,"div",39)(8,"a",40),b(9,"Enviar por WhatsApp"),h(),p(10,"button",41),C("click",function(){R(e);let a=x();return O(a.copiar())}),b(11,"Copiar resultados"),h(),p(12,"div",42)(13,"button",8),C("click",function(){R(e);let a=x();return O(a.descargar("csv"))}),b(14,"Descargar CSV"),h(),p(15,"button",8),C("click",function(){R(e);let a=x();return O(a.descargar("json"))}),b(16,"Descargar JSON"),h()()(),p(17,"mat-card",14)(18,"mat-card-content")(19,"pre",43),b(20),h()()(),p(21,"button",8),C("click",function(){R(e);let a=x();return O(a.reiniciar())}),b(22,"Empezar de nuevo (otra persona)"),h()()}if(t&2){let e=x();v(8),F("href",e.enlaceWhatsApp(),eu),v(12),de(e.textoWhatsApp())}}function hM(t,n){if(t&1){let e=ve();p(0,"a",61),C("click",function(){let a=R(e).$implicit,r=x(2);return r.dispositivo.set(a),r.sintomas.set([]),O(r.ir("p3"))}),p(1,"span",62),b(2),h(),p(3,"span",63),b(4),h()()}if(t&2){let e=n.$implicit;v(2),de(e.nombre),v(2),de(e.zona)}}function fM(t,n){if(t&1){let e=ve();p(0,"a",61),C("click",function(){let a=R(e).$implicit,r=x(2);return O(r.abrir(a))}),p(1,"span",62),b(2),h(),p(3,"span",63),b(4),h(),p(5,"span",64)(6,"span",65),b(7),h()()()}if(t&2){let e=n.$implicit,i=x(2);v(2),de(e.dispositivo),v(2),si("",e.ref," \xB7 ",e.cuando),v(2),S("chip--on",i.indiceEstado(e.estado)>=2),v(),de(e.estado)}}function gM(t,n){if(t&1){let e=ve();p(0,"div",6)(1,"mat-toolbar",44)(2,"span",45),b(3,"Soporte IT"),h()(),p(4,"main",46)(5,"div")(6,"h1",13),b(7,"Hola, Luc\xEDa"),h(),p(8,"p",47),b(9,"Tienda 1234 \xB7 Turno de ma\xF1ana"),h()(),p(10,"mat-card",22)(11,"mat-card-content",48),Ge(),p(12,"svg",49),$(13,"circle",50)(14,"path",51),h(),nt(),p(15,"div")(16,"b"),b(17,"Soporte t\xE9cnico operativo"),h(),p(18,"div",47),b(19,"Sin avisos generales en tu tienda"),h()()()(),p(20,"button",52),C("click",function(){R(e);let a=x();return a.ir("p2"),O(a.escanear())}),Ge(),p(21,"svg",53),$(22,"path",54),h(),b(23,"Escanear etiqueta "),h(),nt(),p(24,"section")(25,"h2",55),b(26,"Tus dispositivos"),h(),p(27,"mat-nav-list"),ut(28,hM,5,2,"a",56,Gx),h()(),p(30,"section")(31,"div",57)(32,"h2",55),b(33,"Incidencias abiertas"),h(),p(34,"button",8),C("click",function(){R(e);let a=x();return O(a.ir("mi"))}),b(35,"Ver todas"),h()(),p(36,"mat-nav-list"),ut(37,fM,8,6,"a",56,dh),h()()(),p(39,"button",58),C("click",function(){R(e);let a=x();return O(a.ir("p2"))}),Ge(),p(40,"svg",53),$(41,"path",59),h(),b(42,"Reportar incidencia "),h(),nt(),xn(43,60),h()}if(t&2){let e=x(),i=et(22);v(28),pt(Ou(1,ZN,e.dispositivos[1],e.dispositivos[3])),v(9),pt(e.abiertas.slice(0,2)),v(6),F("ngTemplateOutlet",i)}}function bM(t,n){if(t&1){let e=ve();p(0,"mat-card",14)(1,"mat-card-content",48),Ge(),p(2,"svg",49),$(3,"circle",50)(4,"path",72),h(),nt(),p(5,"span"),b(6,"En este prototipo la c\xE1mara no est\xE1 disponible. Busca el dispositivo en la lista."),h(),p(7,"button",8),C("click",function(){R(e);let a=x(2);return O(a.avisoEscaneo.set(!1))}),b(8,"Vale"),h()()()}}function vM(t,n){if(t&1){let e=ve();p(0,"a",61),C("click",function(){let a=R(e).$implicit,r=x(2);return O(r.elegir(a))}),p(1,"span",62),b(2),h(),p(3,"span",63),b(4),h()()}if(t&2){let e=n.$implicit;v(2),de(e.nombre),v(2),de(e.zona)}}function _M(t,n){if(t&1&&(p(0,"p",70),b(1),h()),t&2){let e=x(2);v(),zn("Sin resultados para \xAB",e.busqueda(),"\xBB.")}}function yM(t,n){if(t&1){let e=ve();p(0,"div",6)(1,"mat-toolbar",44)(2,"button",66),C("click",function(){R(e);let a=x();return O(a.atras("p1"))}),Ge(),p(3,"svg",49),$(4,"path",67),h()(),nt(),p(5,"span",45),b(6,"Reportar incidencia"),h()(),xn(7,60),p(8,"main",46)(9,"h1",13),b(10,"\xBFQu\xE9 dispositivo falla?"),h(),p(11,"button",52),C("click",function(){R(e);let a=x();return O(a.escanear())}),Ge(),p(12,"svg",53),$(13,"path",54),h(),b(14,"Escanear la etiqueta "),h(),te(15,bM,9,0,"mat-card",14),nt(),p(16,"mat-form-field",68)(17,"mat-label"),b(18,"Buscar por nombre o zona"),h(),p(19,"input",69),vn(),C("ngModelChange",function(a){R(e);let r=x();return O(r.busqueda.set(a))}),h()(),p(20,"section")(21,"h2",55),b(22,"En tu tienda"),h(),p(23,"mat-nav-list"),ut(24,vM,5,2,"a",56,Gx,!1,_M,2,1,"p",70),h()(),p(27,"button",71),C("click",function(){R(e);let a=x();return O(a.noLoSe())}),b(28,"No lo s\xE9 o no est\xE1 en la lista"),h()()()}if(t&2){let e=x(),i=et(18);v(7),F("ngTemplateOutlet",i),v(8),ne(e.avisoEscaneo()?15:-1),v(4),F("ngModel",e.busqueda()),_n(),v(5),pt(e.filtrados())}}function xM(t,n){if(t&1){let e=ve();p(0,"mat-chip-option",81),C("click",function(){let a=R(e).$implicit,r=x(2);return O(r.toggleSintoma(a))}),b(1),h()}if(t&2){let e=n.$implicit,i=x(2);F("selected",i.sintomas().includes(e)),v(),de(e)}}function wM(t,n){if(t&1){let e=ve();p(0,"div",6)(1,"mat-toolbar",44)(2,"button",73),C("click",function(){R(e);let a=x();return O(a.atras("p2"))}),Ge(),p(3,"svg",49),$(4,"path",74),h()(),nt(),p(5,"span",45),b(6,"Reportar incidencia"),h()(),xn(7,60),p(8,"main",46)(9,"div")(10,"p",12),b(11),h(),p(12,"h1",13),b(13,"\xBFQu\xE9 pasa?"),h(),p(14,"p",47),b(15,"Elige lo que se parezca m\xE1s. Puedes marcar varios."),h()(),p(16,"mat-chip-listbox",75),ut(17,xM,2,2,"mat-chip-option",76,yn),h(),p(19,"mat-form-field",16)(20,"mat-label"),b(21,"Detalles (opcional)"),h(),p(22,"textarea",77),vn(),kn("ngModelChange",function(a){R(e);let r=x();return Hn(r.detalles,a)||(r.detalles=a),O(a)}),h()(),p(23,"button",78),b(24,"A\xF1adir foto (opcional)"),h()(),p(25,"div",79)(26,"button",80),C("click",function(){R(e);let a=x();return O(a.ir("p4"))}),b(27,"Continuar"),h()()()}if(t&2){let e=x(),i=et(18);v(7),F("ngTemplateOutlet",i),v(4),de(e.dispositivo()?.nombre),v(6),pt(e.listaSintomas()),v(5),wn("ngModel",e.detalles),_n(),v(4),F("disabled",e.sintomas().length===0)}}function kM(t,n){t&1&&(p(0,"span",85),b(1,"Te sumar\xE1s como afectada al enviar"),h())}function CM(t,n){if(t&1){let e=ve();p(0,"button",8),C("click",function(){R(e);let a=x(3);return O(a.duplicado.set("Es otra"))}),b(1,"Es otra"),h(),p(2,"button",87),C("click",function(){R(e);let a=x(3);return O(a.duplicado.set("Sumarme"))}),b(3,"Sumarme"),h()}}function SM(t,n){if(t&1&&(p(0,"mat-card",14)(1,"mat-card-content")(2,"div",48),Ge(),p(3,"svg",49),$(4,"circle",50)(5,"path",72),h(),nt(),p(6,"div")(7,"b"),b(8,"Puede que ya est\xE9 reportada"),h(),p(9,"div",85),b(10,"INC-20477 \xB7 TPV Caja 2 \xB7 No lee tarjetas \xB7 hace 25 min"),h()()()(),p(11,"mat-card-actions",86),te(12,kM,2,0,"span",85)(13,CM,4,0),h()()),t&2){let e=x(2);v(12),ne(e.duplicado()==="Sumarme"?12:13)}}function EM(t,n){if(t&1){let e=ve();p(0,"div",6)(1,"mat-toolbar",44)(2,"button",73),C("click",function(){R(e);let a=x();return O(a.atras("p3"))}),Ge(),p(3,"svg",49),$(4,"path",74),h()(),nt(),p(5,"span",45),b(6,"Reportar incidencia"),h()(),xn(7,60),p(8,"main",46)(9,"h1",13),b(10,"Impacto y revisi\xF3n"),h(),p(11,"section")(12,"h2",55),b(13,"\xBFPuedes seguir trabajando?"),h(),p(14,"mat-button-toggle-group",82),C("change",function(a){R(e);let r=x();return O(r.impacto.set(a.value))}),p(15,"mat-button-toggle",32),b(16,"S\xED"),h(),p(17,"mat-button-toggle",83),b(18,"Con dificultad"),h(),p(19,"mat-button-toggle",84),b(20,"No, parado"),h()()(),p(21,"section")(22,"h2",55),b(23,"\xBFAfecta a m\xE1s personas?"),h(),p(24,"mat-button-toggle-group",31),C("change",function(a){R(e);let r=x();return O(r.alcance.set(a.value))}),p(25,"mat-button-toggle",32),b(26,"S\xED"),h(),p(27,"mat-button-toggle",33),b(28,"No"),h(),p(29,"mat-button-toggle",34),b(30,"No lo s\xE9"),h()()(),te(31,SM,14,1,"mat-card",14),p(32,"section")(33,"h2",55),b(34,"Resumen"),h(),p(35,"mat-list")(36,"mat-list-item")(37,"span",62),b(38,"Dispositivo"),h(),p(39,"span",63),b(40),h()(),$(41,"mat-divider"),p(42,"mat-list-item")(43,"span",62),b(44,"Qu\xE9 pasa"),h(),p(45,"span",63),b(46),h()()()()(),p(47,"div",79)(48,"button",80),C("click",function(){R(e);let a=x();return O(a.enviar())}),b(49),h()()()}if(t&2){let e=x(),i=et(18);v(7),F("ngTemplateOutlet",i),v(7),F("value",e.impacto()),v(10),F("value",e.alcance()),v(7),ne(e.dispositivo()?.id==="tpv2"&&e.duplicado()!=="Es otra"?31:-1),v(9),de(e.dispositivo()?.nombre),v(6),de(e.sintomas().join(", ")),v(2),F("disabled",!e.impacto()||!e.alcance()),v(),de(e.duplicado()==="Sumarme"?"Sumarme a la incidencia":"Enviar incidencia")}}function DM(t,n){if(t&1){let e=ve();p(0,"div",6)(1,"mat-toolbar",44),$(2,"span",45),h(),p(3,"main",46)(4,"div",88)(5,"div",89),Ge(),p(6,"svg",90),$(7,"path",91),h()(),nt(),p(8,"h1",13),b(9),h(),p(10,"p",47),b(11,"Referencia "),p(12,"b"),b(13),h()()(),p(14,"mat-card",22)(15,"mat-card-content")(16,"b"),b(17,"Qu\xE9 pasar\xE1 ahora"),h(),p(18,"p",47),b(19,"Inform\xE1tica la revisar\xE1, le dar\xE1 prioridad y la asignar\xE1 a un t\xE9cnico. Te avisaremos aqu\xED en cada cambio."),h()()(),xn(20,92),h(),p(21,"div",79)(22,"button",93),C("click",function(){R(e);let a=x();return O(a.terminarT1())}),b(23,"Terminar tarea"),h()()()}if(t&2){let e=x(),i=et(20);v(9),de(e.duplicado()==="Sumarme"?"Te has sumado":"Incidencia enviada"),v(4),de(e.duplicado()==="Sumarme"?"INC-20477":"INC-20482"),v(7),F("ngTemplateOutlet",i)("ngTemplateOutletContext",hc(4,Ux,e.duplicado()==="Sumarme"?1:0))}}function IM(t,n){if(t&1){let e=ve();p(0,"a",98),C("click",function(){let a=R(e).$implicit,r=x(2);return O(r.abrir(a))}),p(1,"span",99),b(2),h(),p(3,"span",62),b(4),h(),p(5,"span",63),b(6),h(),p(7,"span",64)(8,"span",65),b(9),h()()(),$(10,"mat-divider")}if(t&2){let e=n.$implicit,i=x(2);v(2),si("",e.ref," \xB7 ",e.cuando),v(2),de(e.dispositivo),v(2),de(e.sintoma),v(2),S("chip--on",i.indiceEstado(e.estado)>=2),v(),de(e.estado)}}function NM(t,n){if(t&1){let e=ve();p(0,"a",98),C("click",function(){let a=R(e).$implicit,r=x(2);return O(r.abrir(a))}),p(1,"span",99),b(2),h(),p(3,"span",62),b(4),h(),p(5,"span",63),b(6),h(),p(7,"span",64)(8,"span",100),b(9),h()()()}if(t&2){let e=n.$implicit;v(2),si("",e.ref," \xB7 ",e.cuando),v(2),de(e.dispositivo),v(2),de(e.sintoma),v(3),de(e.estado)}}function MM(t,n){if(t&1){let e=ve();p(0,"div",6)(1,"mat-toolbar",44)(2,"span",45),b(3,"Mis incidencias"),h()(),p(4,"mat-tab-group",94),C("selectedIndexChange",function(a){R(e);let r=x();return O(r.tabMI.set(a))}),p(5,"mat-tab",95)(6,"mat-nav-list"),ut(7,IM,11,7,null,null,dh),h()(),p(9,"mat-tab",96)(10,"mat-nav-list"),ut(11,NM,10,5,"a",97,dh),h()()(),p(13,"button",58),C("click",function(){R(e);let a=x();return O(a.ir("p2"))}),b(14,"Reportar incidencia"),h(),xn(15,60),h()}if(t&2){let e=x(),i=et(22);v(4),F("selectedIndex",e.tabMI()),v(),F("label",Ru("Abiertas (",e.abiertas.length,")")),v(2),pt(e.abiertas),v(4),pt(e.cerradas),v(4),F("ngTemplateOutlet",i)}}function TM(t,n){t&1&&(p(0,"button",101),b(1,"Cancelar incidencia"),h(),p(2,"p",102),b(3,"Disponible mientras est\xE9 \xABEnviada\xBB o \xABEn revisi\xF3n\xBB"),h())}function AM(t,n){if(t&1&&(p(0,"main",46)(1,"div",57)(2,"h1",28),b(3),h(),p(4,"span",65),b(5),h()(),p(6,"p",47),b(7),h(),xn(8,92),te(9,TM,4,0),h()),t&2){x();let e=Au(0),i=x(),a=et(20);v(3),de(e.dispositivo),v(),S("chip--on",i.indiceEstado(e.estado)>=2),v(),de(e.estado),v(2),si("",e.sintoma," \xB7 ",e.cuando),v(),F("ngTemplateOutlet",a)("ngTemplateOutletContext",hc(9,Ux,i.indiceEstado(e.estado))),v(),ne(i.indiceEstado(e.estado)<=1?9:-1)}}function RM(t,n){if(t&1){let e=ve();pc(0),p(1,"div",6)(2,"mat-toolbar",44)(3,"button",73),C("click",function(){R(e);let a=x();return O(a.ir("mi"))}),Ge(),p(4,"svg",49),$(5,"path",74),h()(),nt(),p(6,"span",45),b(7),h()(),te(8,AM,10,11,"main",46),h()}if(t&2){let e=Tu(x().seleccionada());v(7),de(e?.ref),v(),ne(e?8:-1)}}function OM(t,n){if(t&1&&(p(0,"div",103)(1,"span",104),b(2),h(),$(3,"mat-progress-bar",105),h()),t&2){let e=x();v(2),zn("Paso ",e.paso()," de 3"),v(),F("value",e.paso()*33.34)}}function FM(t,n){if(t&1&&(p(0,"li",108),$(1,"span",109),p(2,"div")(3,"div",110),b(4),h(),p(5,"div",85),b(6),h()()()),t&2){let e=n.$implicit,i=n.$index,a=x().actual;S("step--done",i<a)("step--now",i===a),v(4),de(e.nombre),v(2),de(i===a?"Ahora":e.ayuda)}}function LM(t,n){if(t&1&&(p(0,"section")(1,"h2",55),b(2,"Seguimiento"),h(),p(3,"ol",106),ut(4,FM,7,6,"li",107,$x),h()()),t&2){let e=x();v(4),pt(e.estados)}}function PM(t,n){if(t&1){let e=ve();p(0,"nav",111)(1,"button",112),C("click",function(){R(e);let a=x();return O(a.ir("p1"))}),p(2,"span",113),Ge(),p(3,"svg",49),$(4,"path",114),h()(),b(5,"Inicio"),h(),nt(),p(6,"button",112),C("click",function(){R(e);let a=x();return O(a.ir("mi"))}),p(7,"span",113),Ge(),p(8,"svg",49),$(9,"path",115),h(),nt(),p(10,"span",116),b(11,"1"),h()(),b(12,"Mis incidencias"),h()()}if(t&2){let e=x();v(),S("on",e.pantalla()==="p1"),v(5),S("on",e.pantalla()==="mi")}}var VM="v1.1 \xB7 2026-11-07",Dl=class t{snack=m(ch);estados=Bx;dispositivos=jx;abiertas=zx;cerradas=Hx;escala7=[1,2,3,4,5,6,7];pantalla=ae("intro");tarea=ae(0);verTarea=ae(!1);consentimiento=!1;alias="";busqueda=ae("");dispositivo=ae(null);sintomas=ae([]);detalles="";impacto=ae("");alcance=ae("");duplicado=ae("");avisoEscaneo=ae(!1);enviando=ae(!1);seleccionada=ae(null);tabMI=ae(0);r=this.nuevo();t0=0;filtrados=at(()=>{let n=this.busqueda().trim().toLowerCase();return n?this.dispositivos.filter(e=>(e.nombre+" "+e.zona).toLowerCase().includes(n)):this.dispositivos});listaSintomas=at(()=>(this.dispositivo()?.id??"").startsWith("tpv")?lh.tpv:lh.otro);paso=at(()=>({p2:1,p3:2,p4:3})[this.pantalla()]??0);nuevo(){let n=Math.random().toString(36).slice(2,8).toUpperCase();return{version:VM,sesion:n,inicio:new Date().toISOString(),pantalla:`${window.innerWidth}x${window.innerHeight}`,alias:"",t1:{ms:0,exito:!1,rendido:!1,dispositivo:"",sintomas:[],impacto:"",alcance:"",duplicado:"",atras:0,escanear:0,noLoSe:0,seq:null},t2:{ms:0,rendido:!1,ruta:[],estado:"",exito:!1,cancelar:"",significado:"",seq:null},umux:{capacidades:null,facilidad:null},confuso:"",mejorar:""}}ir(n){this.pantalla.set(n),this.tarea()===2&&["p1","mi"].includes(n)&&this.r.t2.ruta.push(n),window.scrollTo({top:0})}atras(n){this.tarea()===1&&this.r.t1.atras++,this.ir(n)}empezar(){this.r.alias=this.alias.trim().slice(0,20),this.ir("tarea1")}empezarTarea(n){this.tarea.set(n),this.verTarea.set(!1),this.t0=performance.now(),this.seleccionada.set(null),this.tabMI.set(0),this.ir("p1")}rendirse(){let n=Math.round(performance.now()-this.t0);this.tarea()===1?(this.r.t1.ms=n,this.r.t1.rendido=!0,this.tarea.set(0),this.ir("seq1")):this.tarea()===2&&(this.r.t2.ms=n,this.r.t2.rendido=!0,this.tarea.set(0),this.ir("preg2"))}escanear(){this.tarea()===1&&this.r.t1.escanear++,this.avisoEscaneo.set(!0)}noLoSe(){this.r.t1.noLoSe++,this.snack.open("En el prototipo esta opci\xF3n no avanza. Busca el dispositivo en la lista.","Vale",{duration:4e3})}elegir(n){this.dispositivo.set(n),this.sintomas.set([]),this.ir("p3")}toggleSintoma(n){let e=this.sintomas();this.sintomas.set(e.includes(n)?e.filter(i=>i!==n):[...e,n])}enviar(){let n=Math.round(performance.now()-this.t0),e=this.r.t1;e.ms=n,e.dispositivo=this.dispositivo()?.nombre??"",e.sintomas=this.sintomas(),e.impacto=this.impacto(),e.alcance=this.alcance(),e.duplicado=this.duplicado(),e.exito=this.dispositivo()?.id==="tpv2"&&e.sintomas.includes("No lee tarjetas")&&e.impacto==="No, parado"&&e.alcance==="S\xED",this.ir("p5"),this.snack.open(this.duplicado()==="Sumarme"?"Te has sumado a INC-20477":"Incidencia enviada","Deshacer",{duration:6e3})}terminarT1(){this.tarea.set(0),this.ir("seq1")}abrir(n){this.seleccionada.set(n),this.r.t2.ruta.push("di:"+n.ref),this.ir("di")}loTengo(){this.r.t2.ms=Math.round(performance.now()-this.t0),this.tarea.set(0),this.ir("preg2")}responderT2(){this.r.t2.exito=this.r.t2.estado==="Asignada",this.ir("seq2")}indiceEstado(n){return this.estados.findIndex(e=>e.nombre===n)}CABECERA=["version","sesion","inicio","pantalla","alias","t1_ms","t1_exito","t1_rendido","t1_dispositivo","t1_sintomas","t1_impacto","t1_alcance","t1_duplicado","t1_atras","t1_escanear","t1_nolose","t1_seq","t2_ms","t2_rendido","t2_ruta","t2_estado","t2_exito","t2_cancelar","t2_significado","t2_seq","umux_capacidades","umux_facilidad","confuso","mejorar"];filaCSV(){let n=this.r;return[n.version,n.sesion,n.inicio,n.pantalla,n.alias,n.t1.ms,n.t1.exito?1:0,n.t1.rendido?1:0,n.t1.dispositivo,n.t1.sintomas.join("|"),n.t1.impacto,n.t1.alcance,n.t1.duplicado,n.t1.atras,n.t1.escanear,n.t1.noLoSe,n.t1.seq??"",n.t2.ms,n.t2.rendido?1:0,n.t2.ruta.join(">"),n.t2.estado,n.t2.exito?1:0,n.t2.cancelar,n.t2.significado,n.t2.seq??"",n.umux.capacidades??"",n.umux.facilidad??"",n.confuso,n.mejorar].map(i=>{let a=String(i).replace(/[\r\n]+/g," ");return/[;"]/.test(a)?'"'+a.replace(/"/g,'""')+'"':a}).join(";")}textoWhatsApp(){let n=this.r,e=i=>Math.round(i/1e3)+" s";return[`Test Soporte IT (${n.version}) \xB7 sesi\xF3n ${n.sesion}${n.alias?" \xB7 "+n.alias:""}`,`T1 reportar: ${n.t1.rendido?"se rindi\xF3":n.t1.exito?"OK":"con errores"} \xB7 ${e(n.t1.ms)} \xB7 SEQ ${n.t1.seq??"-"}`,`T2 estado: ${n.t2.rendido?"se rindi\xF3":n.t2.exito?"OK":"fall\xF3"} \xB7 ${e(n.t2.ms)} \xB7 SEQ ${n.t2.seq??"-"}`,`UMUX-Lite: ${n.umux.capacidades??"-"} / ${n.umux.facilidad??"-"}`,n.confuso?`Confuso: ${n.confuso}`:"","","DATOS (no editar):",this.filaCSV()].filter(i=>i!==null).join(`
`)}enlaceWhatsApp=at(()=>(this.pantalla(),"https://wa.me/?text="+encodeURIComponent(this.textoWhatsApp())));async copiar(){try{await navigator.clipboard.writeText(this.textoWhatsApp()),this.snack.open("Resultados copiados",void 0,{duration:2500})}catch{this.snack.open("No se pudo copiar. Mant\xE9n pulsado el texto para copiarlo.",void 0,{duration:4e3})}}async descargar(n){let e=n==="json"?JSON.stringify(this.r,null,2):"\uFEFF"+this.CABECERA.join(";")+`
`+this.filaCSV()+`
`,i=`test-soporte-it_${this.r.sesion}.${n}`,a=window.claude;if(a?.use)try{let o=await a.use("downloads");if(o){await o.save({filename:i,data:e});return}}catch(o){if(o?.code==="declined")return;this.snack.open("Aqu\xED no se puede descargar. Usa \xABCopiar resultados\xBB.",void 0,{duration:4e3});return}let r=document.createElement("a");r.href=URL.createObjectURL(new Blob([e],{type:n==="json"?"application/json":"text/csv;charset=utf-8"})),r.download=i,document.body.appendChild(r),r.click(),r.remove(),setTimeout(()=>URL.revokeObjectURL(r.href),2e3)}reiniciar(){this.r=this.nuevo(),this.tarea.set(0),this.dispositivo.set(null),this.sintomas.set([]),this.impacto.set(""),this.alcance.set(""),this.duplicado.set(""),this.detalles="",this.busqueda.set(""),this.consentimiento=!1,this.alias="",this.ir("intro")}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=B({type:t,selectors:[["app-root"]],decls:23,vars:2,consts:[["progreso",""],["seguimiento",""],["nav",""],[1,"shell"],["role","region","aria-label","Tarea del test",1,"taskbar"],[1,"page","page--script"],[1,"app"],[1,"taskbar__lbl"],["mat-button","",3,"click"],["mat-flat-button","",1,"taskbar__ok"],[1,"taskbar__txt"],["mat-flat-button","",1,"taskbar__ok",3,"click"],[1,"eyebrow"],[1,"headline"],["appearance","outlined",1,"card"],[1,"bullets"],["appearance","outline",1,"full"],["matInput","","maxlength","20","autocomplete","off",3,"ngModelChange","ngModel"],[3,"ngModelChange","ngModel"],[1,"note"],[1,"actions"],["mat-flat-button","",1,"big",3,"click","disabled"],[1,"card","card--filled"],[1,"scenario"],["mat-flat-button","",1,"big",3,"click"],["aria-label","Facilidad de 1 a 7",1,"scale",3,"change","value"],[3,"value"],[1,"scale__ends"],[1,"title"],[1,"radios",3,"ngModelChange","ngModel"],["value","No lo encontr\xE9"],[1,"full",3,"change","value"],["value","S\xED"],["value","No"],["value","No lo s\xE9"],["appearance","outline",1,"full","top"],["matInput","","rows","2","maxlength","200",3,"ngModelChange","ngModel"],[1,"scale",3,"change","value"],["matInput","","rows","2","maxlength","300",3,"ngModelChange","ngModel"],[1,"actions","actions--stack"],["mat-flat-button","","target","_blank","rel","noopener",1,"big",3,"href"],["mat-stroked-button","",1,"big",3,"click"],[1,"row"],[1,"pre"],[1,"topbar"],[1,"topbar__title"],[1,"page"],[1,"muted"],[1,"inline"],["viewBox","0 0 24 24",1,"ic"],["cx","12","cy","12","r","9"],["d","M8 12l3 3 5-6"],["mat-flat-button","",1,"tonal","big","full",3,"click"],["viewBox","0 0 24 24",1,"ic","ic--btn"],["d","M4 8V5h3M17 5h3v3M20 16v3h-3M7 19H4v-3M8 12h8"],[1,"label-lg"],["mat-list-item",""],[1,"sec"],["mat-fab","","extended","",1,"fab",3,"click"],["d","M12 5v14M5 12h14"],[3,"ngTemplateOutlet"],["mat-list-item","",3,"click"],["matListItemTitle",""],["matListItemLine",""],["matListItemMeta","",1,"meta"],[1,"chip"],["mat-icon-button","","aria-label","Cerrar",3,"click"],["d","M6 6l12 12M18 6L6 18"],["appearance","outline","subscriptSizing","dynamic",1,"full","search"],["matInput","","autocomplete","off",3,"ngModelChange","ngModel"],[1,"muted","pad"],["mat-button","",1,"center",3,"click"],["d","M12 11v5M12 8h0"],["mat-icon-button","","aria-label","Atr\xE1s",3,"click"],["d","M20 12H4m6-6l-6 6 6 6"],["multiple","","aria-label","S\xEDntomas"],[3,"selected"],["matInput","","rows","3","maxlength","300",3,"ngModelChange","ngModel"],["mat-stroked-button","",1,"start"],[1,"bar"],["mat-flat-button","",1,"big","full",3,"click","disabled"],[3,"click","selected"],["hideSingleSelectionIndicator","false",1,"full",3,"change","value"],["value","Con dificultad"],["value","No, parado"],[1,"muted","small"],["align","end"],["mat-flat-button","",1,"tonal",3,"click"],[1,"hero"],[1,"hero__ill"],["viewBox","0 0 24 24"],["d","M5 12l5 5L20 7"],[3,"ngTemplateOutlet","ngTemplateOutletContext"],["mat-flat-button","",1,"big","full",3,"click"],["mat-stretch-tabs","",3,"selectedIndexChange","selectedIndex"],[3,"label"],["label","Cerradas"],["mat-list-item","","lines","3"],["mat-list-item","","lines","3",3,"click"],["matListItemOverline",""],[1,"chip","chip--on"],["mat-stroked-button","",1,"big","full"],[1,"muted","small","center-txt"],[1,"progress"],[1,"label-md"],["mode","determinate",3,"value"],[1,"steps"],[1,"step",3,"step--done","step--now"],[1,"step"],["aria-hidden","true",1,"step__dot"],[1,"step__name"],["aria-label","Navegaci\xF3n principal",1,"navbar"],[1,"navbar__item",3,"click"],[1,"pill"],["d","M4 11l8-7 8 7v9h-5v-6H9v6H4z"],["d","M9 6h11M9 12h11M9 18h11M4.5 6h0M4.5 12h0M4.5 18h0"],[1,"badge"]],template:function(e,i){if(e&1&&(p(0,"div",3),te(1,eM,9,5,"div",4),te(2,tM,32,3,"main",5)(3,nM,19,0,"main",5)(4,aM,16,2,"main",5)(5,rM,17,0,"main",5)(6,sM,26,4,"main",5)(7,lM,16,2,"main",5)(8,uM,34,5,"main",5)(9,pM,23,2,"main",5)(10,gM,44,4,"div",6)(11,yM,29,4,"div",6)(12,wM,28,4,"div",6)(13,EM,50,8,"div",6)(14,DM,24,6,"div",6)(15,MM,16,4,"div",6)(16,RM,9,3,"div",6),h(),It(17,OM,4,2,"ng-template",null,0,ci)(19,LM,6,0,"ng-template",null,1,ci)(21,PM,13,4,"ng-template",null,2,ci)),e&2){let a;v(),ne(i.tarea()>0?1:-1),v(),ne((a=i.pantalla())==="intro"?2:a==="tarea1"?3:a==="seq1"?4:a==="tarea2"?5:a==="preg2"?6:a==="seq2"?7:a==="umux"?8:a==="fin"?9:a==="p1"?10:a==="p2"?11:a==="p3"?12:a==="p4"?13:a==="p5"?14:a==="mi"?15:a==="di"?16:-1)}},dependencies:[P_,Lc,F_,op,lp,Ur,tl,el,Sp,hy,gy,fy,_y,vy,Ly,Lp,Fp,jy,Vp,dl,Wy,Uy,Gy,$y,Bp,zp,jp,Hp,Ky,qy,Zy,Yy,co,sl,ro,tx,ex,nx,Gp,ax,Wp,hl,fx,eh,hx,Vx,ml],encapsulation:2})};Ku(Dl,o_).catch(t=>console.error(t));
