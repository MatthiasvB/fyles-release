# Why I built a cross-platform file sharing application

It all started with my annoying Synology NAS that spun up its HDDs, audibly, every 20 minutes, with no way to disable it. That needed changing, but it wasn't high priority.

Then I updated my work machine to Windows 11 (I'm a frontend dev). I hated the fact that I could no longer ungroup icons of multiple instances of a single application on the taskbar. Obviously, I had to move to Linux now. There were some other reasons, too.

A colleague of mine had been praising NixOS _a lot_. My company is pretty cool, so I decided &hyphen; and was allowed &hyphen; to give that a try (props to MATHEMA GmbH).

After a year or so on NixOS, I felt comfortable enough to try and convert an old laptop of mine into a homeserver running NixOS to replace the NAS.

I tried Nextcloud (the NixOS module is easy to set up but tough to keep running) and ended up being pretty happy with Syncthing to sync my devices. I loved it for what it does, but there was one use case that was hard to solve: How to easily send files to my girlfriend in the next room?

Sure, I can place it on a Samba share (but she doesn't like accessing those on her phone). We could use QuickShare or LocalSend, but both of us would need to grab our phones &hyphen; me to send and her to accept. Or she could add me to the permanent allow list, but LocalSend doesn't have the crypto to make that secure. We could share a Syncthing folder, but it's too much setup for ad-hoc file transfers (not an issue with my girlfriend, but I'm not going to create a directory for every acquaintance). Also, I'd have to keep the copy on my phone until she accepted it (and removed it from the shared folder). Also, the Android client is kind of in limbo. And it doesn't (really) work for iOS. What does, to be fair.

The simple use case of sending a file to a specific device of my non-IT girlfriend, at any time, without her having to accept it, broke me.

I figured there are so many situations in our lives in which we may want other people to send files to us &hyphen; weddings, birthday parties, or potentially homework in school. These would be files scoped to a particular purpose or event. Or we may want to simply accept files from certain people at any time and for any purpose. These are the use cases I considered when designing "Fyles". Also, I didn't want these files to go through a cloud provider just to travel to the next room in the real world.

## Filerequests

Essentially, I wanted to recreate the “send me a link where I can drop files” scenario we know from the cloud. Only fully managed on client devices, secure, and absolutely private. Conceptually, Fyles has these links. Only they're not links &hyphen; they're called Filerequests, and &hyphen; like the links &hyphen exist in two flavors: private and public.

### Public filerequests

Public filerequests are the analog of the “anybody with this link can edit” shares. They are super flexible, but they shouldn't get into the wrong hands. Personally, I’d only use them for a limited time, like the duration of a party. Put a QR on the wall, and anybody can send photos. They’re not meant to circulate for long, as a malicious actor could use them to put arbitrary files on your devices. Only in trusted crowds, so to speak. But for that use case, they are great!

### Private filerequests

When I want to enable my girlfriend to send me files at any time, I want to be sure those are actually sent by her. So we'd exchange contact information (→ public keys), and I would authorize her to send me files via a private filerequest. Those can live longer and cryptographically lock in to a select group of contacts who can use them to send files.

## How does it work

For now, Fyles uses mDNS, implemented via libp2p, to discover peers on the local network and exchange files with them. In the future, I'd love to enable transfers over the Internet or without any network at all (Bluetooth, Wi-Fi Direct style), but both approaches come with significant challenges. So, for now, the focus is local transfers only.

And so it pretty much comes down to discovering peers via mDNS, figuring out whether I need to send files to any of them, or accept files from others. In order to do so, we need to identify contacts. While libp2p allows identifying _peers_, that's not good enough, because Mike’s phone and Mike’s laptop would be different peers, and I don’t want to know each of Mike’s devices individually to accept files from any of them.

### Post-quantum cryptography

So, to identify Mike on any of his devices, I needed a verifiable contact ID. I needed to build some crypto. And since I needed to dig into that rabbit hole, I dug deep, and what happens now is similar to a Diffie–Hellman key exchange. Only we send two ephemeral keys: one is the classic X25519, the other is a Kyber KEM (quantum safe). And we don’t send them plainly &hyphen; we send both of them signed via our contact ID keys. One of them is Ed25519, the other is Dilithium (quantum safe). We also send our contact ID (just a UUID). The other end:

- checks whether they know that contact ID
- if so, they use the public keys they already know from us to verify the signatures
- only if both signatures match do they trust that we are who we say we are
- generate the data to complete the key exchange with, and sign it with their crypto material
- generate the shared secret and respond to Diffie–Hellman

And then we can similarly verify that the other end is who we think they are. If any party does not know the other, the session is still established, but it’s not a trusted one (on at least one end). This might work for public filerequests, but not private ones. If the contact IDs can be verified, both ends establish a trusted session. The shared secret obtained from the dual key exchange is combined, and a key for symmetric encryption is derived from it. Symmetric encryption is not (very) vulnerable to quantum attacks, so by encrypting all following communication with that symmetric key, we are pretty safe from eavesdropping, whether we trust each other or not. Also, everything is encrypted on the transport layer via Noise as well, to provide an extra layer of security.

Of course, all of this is a bit overkill. But it turns out to be not that much more effort to send two keys instead of one, and once Fyles moves on to communication over the Internet, the quality of the encryption will become a lot more relevant.

## The tech stack

I’m just one guy, and I have a job and a life, and still I wanted to create this app. So from the start, I focused heavily on a tech stack that would run cross-platform. I chose Flutter for the frontend. That’s pretty obvious and boring. It gets more interesting with the “backend” logic.

### libp2p

Pretty soon it became clear that implementing all the P2P protocols myself would be a nightmare. But there is libp2p which, though still under development, promised to allow for some shortcuts. It has official implementations in JS, Go, and Rust, and more are coming. But none of these scream “I will run on any platform.” I mean, what does? Certainly not JS. I expected the best chances with compiled Go and Rust. I had run a small server binary built with Go in Termux on an old phone, so at least that worked.

### Dabbling with Go

I had wanted to give Go a try for a while. I had written some tiny things that worked, but it hadn’t clicked. But it seemed much easier than Rust, which I was more familiar with (think: able to do some of the Advent of Code challenges in it). So I tried that.

Sure enough, you get some basic functionality pretty quickly. But it is just not a language that I am comfortable with. Not expressive enough. And this was going to get complex, so I really wanted that expressiveness. Then I also ran into trouble with some stuff not working on Android. I believe it was related to DNS, but I may be wrong. My research showed that some foundational library just wasn’t ready for Android. Maybe a workaround might have been possible, but getting into ugly hacks that early seemed wrong. I was very much still experimenting with basic libp2p functionality. So I decided to move to Rust.

### The mess that is Rust

Very soon after I started, I realized that Rust was going to be _a lot_ harder. Not only is the language much more difficult, the libp2p implementation is also much more low-level. The different language implementations may implement the same protocols (with varying success), but the APIs they expose are drastically different. And the Rust implementation is essentially a huge event loop, where even the "request-response" protocol is split between several different events, and you need to track request IDs manually to associate the response you get with the request you sent &hyphen; if you need that info. I think to this day I do not fully understand what information the library tracks internally and what I have to either track myself or explicitly pass to it. But I’m sure that if I ever move towards protocols that warrant that degree of flexibility, I will be thankful for it. For now, it just runs. And that it does &hyphen; at least with one or two patches on my own fork.

### The beauty that is Rust

I love the Rust language, but it took months to reproduce what I coded in a few weeks in Go. But what I got is a cleanly architected, excellently tested, efficiently running application that runs with only one hiccup (again DNS on Android) on Windows, macOS, Linux, Android, and iOS. DNS resolution is kinda broken on Android (no `/etc/resolv.conf`), but conditional compilation easily allows hot-fixing that by simply setting the nameserver to Google’s `8.8.8.8` on Android for now, and it runs happily. It runs as a daemon on Windows. It ran as a subprocess on Android. And when I moved to iOS and subprocesses ceased being an option, I simply encapsulated the entire logic in a library crate that communicates with its surroundings via a single `HostController` trait. This can be created from the daemon crate, the JVM FFI layer for Android, or the C FFI layer for Swift on macOS and iOS. Starting the app, as well as any interactions with the intricate, permission-sensitive filesystem access APIs on mobile platforms, can be handled in platform code, while the core business logic remains unchanged, cross-platform Rust that just runs.

### Talking to the frontend

So how do we get messages across between the Flutter frontend and the Rust backend? While they are the same process on iOS, they are an Activity and a Foreground Service on Android, and a frontend and a daemon on Windows. While this isn’t finished on Windows, the answer on Android and iOS is gRPC over a Unix domain socket. I used to use gRPC over `127.0.0.1:<port>`, but that causes all sorts of concerns on multi-user machines. A Unix domain socket can simply be put on the filesystem and benefits from filesystem-level access control. Problem solved. I can keep using a port on loopback for easy development, but production is secure.
